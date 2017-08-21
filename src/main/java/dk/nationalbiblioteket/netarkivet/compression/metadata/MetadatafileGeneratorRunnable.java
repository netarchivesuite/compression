package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.AlreadyKnownMissingFileException;
import dk.nationalbiblioteket.netarkivet.compression.tools.ValidateMetadataOutput;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileCache;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileCacheFactory;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileLoaderImpl;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileTriLongLoaderImpl;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriter;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriterArc;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriterWarc;
import dk.netarkivet.wayback.batch.DeduplicateToCDXAdapter;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.format.CDXFormat;
import org.archive.wayback.resourceindex.cdx.format.CDXFormatException;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.ANVLRecord;
import org.jwat.common.HeaderLine;
import org.jwat.common.Payload;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * Created by csr on 1/4/17.
 */
public class MetadatafileGeneratorRunnable implements Runnable {

    private final BlockingQueue<String> sharedQueue;
    private int threadNo;
    private static boolean isDead = false;
    Logger logger = LoggerFactory.getLogger(MetadatafileGeneratorRunnable.class);
    private final static String dedupURI = "metadata://crawl/index/deduplicationmigration?majorversion=0&minorversion=0";
    private final static String dedupCdxURI = "metadata://crawl/index/deduplicationcdx?majorversion=0&minorversion=0";

    /**
     *
     * @param sharedQueue
     * @param threadNo
     */
    public MetadatafileGeneratorRunnable(BlockingQueue<String> sharedQueue, int threadNo) {
        System.setProperty("settings.harvester.harvesting.metadata.compression", "true");
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
        logger.info("Generating metadata using caching implementation: {}.", getIFileCache().getClass().getName() );
    }

    /**
     * Takes a filename corresponding to an old-style uncompressed metadata file and creates a new
     * compressed metadata file with i) modified cdx records and ii) a new deduplication-info record.
     * Also renames the original file, replacing "metadata" in the name with "oldmetadata".
     * If Util.NMETADATA_DIR is not null or empty then the filename is interpreted relative to that. Otherwise it
     * is relative to current working dir.
     *
     * If the file has already been reprocessed, then this method returns false
     *
     * @param filename
     * @throws NoSuchFileException if filename does not exist
     * @return whether or not the file was processed
     */
    boolean processFile(String filename) throws IOException, WeirdFileException, DeeplyTroublingException {
        Properties properties = Util.getProperties();
        String inputPath = properties.getProperty(Util.METADATA_DIR);
        File inputFile;
        if (inputPath != null)  {
            inputFile = new File(inputPath, filename);
        } else {
            inputFile = new File(filename);
        }
        logger.info("Thread #{}: Processing file {} ", threadNo, inputFile.getAbsolutePath());
        final String replacementFilename = inputFile.getName().replace("metadata", "oldmetadata");
        final File dest = new File(inputFile.getParentFile(), replacementFilename);
        if (dest.exists()) {
            logger.info("Thread #{}: Found existing output file {} so skipping. ", threadNo, dest.getAbsolutePath());
            return false;
        }
        if (!inputFile.exists()) {
            throw new NoSuchFileException("Input file " + inputFile.getAbsolutePath() + " not found, and file has not already been processed.");
        }
        String outputDir = properties.getProperty(Util.NMETADATA_DIR);
        if (outputDir == null || outputDir.trim().length() == 0) {
            outputDir = inputFile.getParentFile().getAbsolutePath();
        } else {
            File outputDirFile = Util.getNMetadataSubdir(inputFile.getName(), true);
            outputDir = outputDirFile.getAbsolutePath();
        }
        final Path outputDirPath = Paths.get(outputDir);
        Files.createDirectories(outputDirPath);
        File cdxDir = Util.getCDXSubdir(inputFile.getName(), true);

        final String newMetadataFilename = Util.getNewMetadataFilename(filename);
        if (newMetadataFilename == null) {
            throw new WeirdFileException("Cannot work out how to generate new metadata file name from " + filename);
        }
        Path outputFilePath = outputDirPath.resolve(newMetadataFilename);
        final File dedupCdxFile = new File(cdxDir, inputFile.getName() + ".cdx");
        boolean foundCrawlLog = true;
        if (filename.endsWith(".warc") || filename.endsWith(".warc.gz")) {
            foundCrawlLog = processWarcfile(inputFile, outputFilePath.toFile(), dedupCdxFile);
        } else if (filename.endsWith(".arc") ||filename.endsWith(".arc.gz")) {
            foundCrawlLog = processArcfile(inputFile, outputFilePath.toFile(), dedupCdxFile);
        } else {
            throw new WeirdFileException("Input metadata file is neither arc nor warc: " + filename);
        }
        boolean renameIsSuccessful = inputFile.renameTo(dest);
        if (!renameIsSuccessful) {
            throw new DeeplyTroublingException("Unable to rename input file '" + inputFile.getAbsolutePath() + "' as '"
                    + dest.getAbsolutePath() + "'. Unknown reason");
        } else {
            logger.info("Thread #{}: Renamed successfully input file '{}' as '{}'", threadNo, inputFile.getAbsolutePath(), dest.getAbsolutePath() );
        }
        writeMD5UpdatedFilename(dest);
        File newMetadataFile = outputFilePath.toFile();
        writeMD5UpdatedFilename(newMetadataFile);
        if (foundCrawlLog) {
            boolean isValid = validateMetadataFileGeneration(dest, newMetadataFile, dedupCdxFile);
            if (!isValid) {
                logger.warn("Thread #{}: Metadata validation of output of '{}' failed", threadNo, inputFile.getAbsolutePath());
            }
        } else {
            // There's really no need to validate anything here. The metadata is just a record of a job that never ran for
            // some reason. It has no reference to any archival content.
            logger.info("Thread #{}: No crawl log was found in {} so no validation was carried out. Will create placeholder cdx file.", threadNo, inputFile.getAbsolutePath());
            logger.debug("Thread #{}: Creating dedup cdx file {}.", threadNo, dedupCdxFile.getAbsolutePath());
            dedupCdxFile.createNewFile();
        }

        logger.info("Thread #{}: Finished done processing file '{}'", threadNo, inputFile.getAbsolutePath());
        logger.trace(Util.getMemoryStats());
        return true;
    }

    /**
     * Compare the oldmetadata file with the new one.
     * @param originalMetadataFile
     * @param newMetadataFile
     * @param dedupCdxFile
     * @return true if no warnings, else false
     */
    private boolean validateMetadataFileGeneration(File originalMetadataFile, File newMetadataFile, File dedupCdxFile) {
        int warnings = 0;
        if (originalMetadataFile.length() >= newMetadataFile.length()) {
            logger.warn("Thread #{}: Very surprised to find that new metadata file {} is smaller than old metadata file {}.", threadNo, newMetadataFile.getAbsolutePath(), originalMetadataFile.getAbsolutePath());
            warnings++;
        }
        // Validate that we have 2 records more in the newMetadataFile than in the original
        int recordDiff = ValidateMetadataOutput.getRecordDiff(originalMetadataFile, newMetadataFile);
        if (recordDiff != 2) {
            logger.warn("Thread #{}: Found unexpected difference between record number in original '{}' and new metadata file '{}'. Expected 2 more records in the new file",
                    threadNo, originalMetadataFile.getAbsolutePath(), newMetadataFile.getAbsolutePath());
            warnings++;
        }
        if (!dedupCdxFile.exists()) {
            logger.warn("Thread #{}: dedupCdxFile {} does not exist. Creating empty cdxfile", threadNo, dedupCdxFile);
            boolean filecreated = false;
            try {
                filecreated = dedupCdxFile.createNewFile();
            } catch (IOException e) {
                logger.warn("Thread #{}: Unable to create empty file '{}'", threadNo, dedupCdxFile.getAbsolutePath());
                warnings++;
            }
            if (!filecreated) {
                logger.warn("Thread #{}: Unable to create empty file '{}'", threadNo, dedupCdxFile.getAbsolutePath());
                warnings++;
            }
        } else {
            // Compare contents of dedupcxdfile with # of duplicate linies in crawl.log
            boolean compareIsOK = false;
            try {
                compareIsOK = ValidateMetadataOutput.compareCrawllogWithDedupcdxfile(originalMetadataFile, newMetadataFile, dedupCdxFile);
            } catch (IOException e) {
                logger.warn("Thread #{}: Validation of contents of dedupcdxfile and the metadata files failed", threadNo, dedupCdxFile.getAbsolutePath(), e);
                warnings++;

            }
            if (!compareIsOK) {
                logger.warn("Thread #{}: Validation of contents of dedupcdxfile and the metadata files does not match", threadNo, dedupCdxFile.getAbsolutePath());
                warnings++;
            }

        }

        return warnings == 0;
    }

    /**
     *
     * @param input
     * @param output
     * @param dedupCdxFile
     * @return true iff a crawl log was found
     * @throws IOException
     * @throws DeeplyTroublingException
     */
    private boolean processArcfile(File input, File output, File dedupCdxFile) throws IOException, DeeplyTroublingException {
        logger.info("Thread #{}: Processing ARC input file {} with outputfile {}.", threadNo, input.getAbsolutePath(), output.getAbsolutePath());
        boolean foundCrawlLog = false;
        MetadataFileWriter writer = null;
        try (
                InputStream is = new FileInputStream(input);
                ArcReader reader = ArcReaderFactory.getReader(is);
        ) {
            final Iterator<ArcRecordBase> iterator = reader.iterator();
            writer = MetadataFileWriterArc.createWriter(output);
            while (iterator.hasNext()) {
                ArcRecordBase recordBase = iterator.next();
                byte[] payload = new byte[]{};
                String url = recordBase.getUrlStr();
                logger.debug("Thread #{}: Processing {} from {}.", threadNo, url, input.getAbsolutePath());
                if (url.contains("filedesc://")) {
                    continue;
                }
                Payload oldPayload = null;
                File oldPayloadFile = File.createTempFile("arcrecord", "txt");
                try {
                    if (recordBase.hasPayload()) {
                        oldPayload = recordBase.getPayload();
                    }
                    if (oldPayload != null) {
                        try (InputStream oldPayloadIS = oldPayload.getInputStreamComplete()) {
                            Files.copy(oldPayloadIS, oldPayloadFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                        }
                        if (url.contains("crawl.log")) {
                            foundCrawlLog = true;
                            byte[][] dedupPayload = null;
                            try (InputStream oldPayloadIS = new FileInputStream(oldPayloadFile)) {
                                dedupPayload = getDedupPayload(oldPayloadIS, input.getAbsolutePath());
                            }
                            writer.write(dedupURI, "text/plain", recordBase.getIpAddress(),
                                    System.currentTimeMillis(), dedupPayload[0]);
                            logger.debug("Thread #{}: Writing {} bytes to dedupmigration for {}.", threadNo, dedupPayload[0].length, output.getAbsolutePath());
                            writer.write(dedupCdxURI, "text/plain", recordBase.getIpAddress(), System.currentTimeMillis(), dedupPayload[1]);
                            logger.debug("Thread #{}: Writing {} bytes to dedupcdx for {}.", threadNo, dedupPayload[1].length, output.getAbsolutePath());
                            logger.debug("Thread #{}: Creating dedup cdx file {}.", threadNo, dedupCdxFile.getAbsolutePath());
                            if (dedupPayload[1].length > 0) {
                                FileUtils.writeByteArrayToFile(dedupCdxFile, dedupPayload[1]);
                            } else {
                                dedupCdxFile.createNewFile();
                            }
                        }
                        if (url.contains("index/cdx")) {
                            try (InputStream oldPayloadIS = new FileInputStream(oldPayloadFile)) {
                                payload = getUpdatedCdxPayload(oldPayloadIS, input.getAbsolutePath());
                            }
                            url = url.replace(".arc", ".arc.gz");
                            logger.debug("Thread #{}: Writing {} bytes to migrated cdx for {}.", threadNo, payload.length, output.getAbsolutePath());
                            writer.write(url, recordBase.getContentTypeStr(),
                                    recordBase.getIpAddress(), System.currentTimeMillis(), payload);
                        } else {
                            writer.writeTo(oldPayloadFile, url, recordBase.getContentTypeStr());
                        }
                    } else {  //no payload
                        logger.debug("Thread #{}: Writing empty record for {}.", threadNo, url);
                        writer.writeTo(oldPayloadFile, url, recordBase.getContentTypeStr());
                    }
                } finally {
                    oldPayloadFile.delete();
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        logger.info("Thread #{}: Finished processing ARC input file {}", threadNo, input.getAbsolutePath());
        return foundCrawlLog;
    }

    /**
     *
     * @param input
     * @param output
     * @param dedupCdxFile
     * @return true iff a crawl log was found
     * @throws IOException
     * @throws DeeplyTroublingException
     */
    private boolean processWarcfile(File input, File output, File dedupCdxFile) throws IOException, DeeplyTroublingException {
        logger.info("Thread #{}: Processing WARC input file {} with outputfile {}.", threadNo, input.getAbsolutePath(), output.getAbsolutePath());
        boolean foundCrawlLog = false;
        MetadataFileWriter writer = null;
        try (
                InputStream is = new FileInputStream(input);
                WarcReader reader = WarcReaderFactory.getReader(is);
        ){
            final Iterator<WarcRecord> iterator = reader.iterator();
            writer = MetadataFileWriterWarc.createWriter(output);
            while (iterator.hasNext()) {
                WarcRecord record = iterator.next();
                if (record.getHeader(WarcConstants.FN_WARC_TYPE).value.equals("warcinfo")) {
                    ANVLRecord infoPayload = new ANVLRecord();
                    infoPayload.addLabelValue("replaces", record.getHeader(WarcConstants.FN_WARC_FILENAME).value);
                    infoPayload.addValue(IOUtils.toString(record.getPayloadContent())); // TODO IOUtils.toString(InputStream deprecated; use IOUtils.toString(InputStream,Charset) instead 
                    ((MetadataFileWriterWarc) writer).insertInfoRecord(infoPayload);
                } else if (record.getHeader(WarcConstants.FN_WARC_TYPE).value.equals("resource")) {
                    final HeaderLine uriLine = record.getHeader(WarcConstants.FN_WARC_TARGET_URI);
                    final HeaderLine contentTypeLine = record.getHeader(WarcConstants.FN_CONTENT_TYPE);
                    final HeaderLine hostIpLine = record.getHeader(WarcConstants.FN_WARC_IP_ADDRESS);
                    byte[] newPayloadBytes = new byte[]{};
                    Payload oldPayload = null;
                    File oldPayloadFile = File.createTempFile("warcrecord", "txt");
                    try {
                        if (record.hasPayload()) {
                            oldPayload = record.getPayload();
                        }
                        if (oldPayload != null) {
                            try (InputStream oldPayloadIS = oldPayload.getInputStreamComplete()) {
                                Files.copy(oldPayloadIS, oldPayloadFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                            }
                            //Note that in the case of the crawl log we add two new records, whereas in the case of
                            //the cdx records we alter the record.
                            if (uriLine.value.contains("crawl.log")) {
                                foundCrawlLog = true;
                                byte[][] dedupPayload = null;
                                try (InputStream oldPayloadIS = new FileInputStream(oldPayloadFile)) {
                                    dedupPayload = getDedupPayload(oldPayloadIS, input.getAbsolutePath());
                                }
                                writer.write(dedupURI, "text/plain", valueOrNull(hostIpLine), System.currentTimeMillis(), dedupPayload[0]);
                                logger.debug("Thread #{}: Writing {} bytes to dedupmigration record for {}.", threadNo, dedupPayload[0].length, output.getAbsolutePath());
                                writer.write(dedupCdxURI, "text/plain", valueOrNull(hostIpLine), System.currentTimeMillis(), dedupPayload[1]);
                                logger.debug("Thread #{}: Writing {} bytes to deduplicationcdx record for {}.", threadNo, dedupPayload[1].length, output.getAbsolutePath());
                                logger.debug("Thread #{}: Creating dedup cdx file {}.", threadNo, dedupCdxFile.getAbsolutePath());
                                if (dedupPayload[1].length > 0) {
                                    FileUtils.writeByteArrayToFile(dedupCdxFile, dedupPayload[1]);
                                } else  {
                                    dedupCdxFile.createNewFile();
                                }
                                writer.writeTo(oldPayloadFile, valueOrNull(uriLine), valueOrNull(contentTypeLine));
                            } else if (uriLine.value.contains("index/cdx")) {
                                uriLine.value = uriLine.value.replace("arc", "arc.gz");
                                try (InputStream oldPayloadIS = new FileInputStream(oldPayloadFile)) {
                                    newPayloadBytes = getUpdatedCdxPayload(oldPayloadIS, input.getAbsolutePath());
                                }
                                logger.debug("Thread #{}: Writing {} bytes to migrated cdx for {}.", threadNo, newPayloadBytes.length, output.getAbsolutePath());
                                writer.write(valueOrNull(uriLine),
                                        valueOrNull(contentTypeLine),
                                        valueOrNull(hostIpLine), System.currentTimeMillis(), newPayloadBytes);
                            } else {
                                writer.writeTo(oldPayloadFile, valueOrNull(uriLine), valueOrNull(contentTypeLine));
                            }
                        } else {
                            writer.writeTo(oldPayloadFile, valueOrNull(uriLine), valueOrNull(contentTypeLine));
                        }
                    } finally {
                        oldPayloadFile.delete();
                    }
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        logger.info("Thread #{}: Finished processing WARC input file {}", threadNo, input.getAbsolutePath());
        return foundCrawlLog;
    }

    private byte[] getUpdatedCdxPayload(InputStream cdxPayloadIS, String inputPath) throws DeeplyTroublingException, FileNotFoundException {
        String cdxSpec = " CDX A r b m S g V k";
        CDXFormat cdxFormat = null;
        try {
            cdxFormat = new CDXFormat(cdxSpec);
        } catch (CDXFormatException e) {
            throw new RuntimeException(e);
        }
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(cdxPayloadIS));
            boolean firstLine = true;
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null && line.trim().length() > 0) {
                try {
                    CaptureSearchResult captureSearchResult = null;
                    captureSearchResult = cdxFormat.parseResult(line);
                    String file = captureSearchResult.getFile();
                    long oldOffset = captureSearchResult.getOffset();
                    IFileCache iFileCache = getIFileCache();
                    try {
                        IFileEntry iFileEntry = iFileCache.getIFileEntry(file, oldOffset);
                        captureSearchResult.setOffset(iFileEntry.getNewOffset());
                        captureSearchResult.setFile(file + ".gz");
                        line = cdxFormat.serializeResult(captureSearchResult);
                    } catch (Exception e) {
                        // If there is no lookup for this record, it may be because it refers to a record
                        // from a file which cannot be compressed. But the file may stil have valid archive data
                        // so we just write the cdx record back unmodified. This has the side effect that the
                        // new metadata file is still guaranteed to be larger than the original.
                        logger.warn("Error processing '{}' in {} so just writing it back to output", line, inputPath, e);
                    }
                    if (!firstLine) {
                        sb.append("\n");
                    }
                    sb.append(line);
                    firstLine = false;
                } catch (AlreadyKnownMissingFileException e) {
                    //logger.warn("Error processing '" + line + "'", e);
                } catch (Exception e) {
                    logger.warn("Error processing line '{}' in {}.", line, inputPath, e);
                }

 /*               catch (Exception e) {
                    if (e.getCause() instanceof AlreadyKnownMissingFileException) {
                        //Do nothing
                    } else if (e.getCause() instanceof FileNotFoundException) {
                        logger.warn("Missing ifile. {}.", e.getMessage());
                    } else
                        logger.warn("Error processing '" + line + "'", e);
                }*/
            }
            return sb.toString().getBytes();
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        } finally {
            IOUtils.closeQuietly(br);
        }
    }
    /**
     *
     * @return
     * @throws DeeplyTroublingException
     */
    private IFileCache getIFileCache() {
        if (Boolean.parseBoolean(Util.getProperties().getProperty(Util.USE_SOFT_CACHE))) {
            return IFileCacheFactory.getIFileCache(new IFileTriLongLoaderImpl());
        } else {
            return  IFileCacheFactory.getIFileCache(new IFileLoaderImpl());
        }
    }

    private byte[][] getDedupPayload(InputStream crawllogPayloadIS, String originalFilePath) throws DeeplyTroublingException {
        final IFileCache iFileCache = getIFileCache();
        StringBuffer migrationOutput = new StringBuffer();
        StringBuffer cdxOutput = new StringBuffer();
        int dedupEntriesFound=0;
        int dedupEntriesFailed=0;
        DeduplicateToCDXAdapter adapter = new DeduplicateToCDXAdapter();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(crawllogPayloadIS))) {
            boolean firstLine = true;
            String line;
            while ((line = br.readLine()) != null && line.trim().length() > 0) {
                if (line.contains("duplicate:")) {
                    dedupEntriesFound++;
                    try {
                        String original = adapter.adaptLine(line);
                        if (original != null) {
                            if (!firstLine) {
                                cdxOutput.append("\n");
                                migrationOutput.append("\n");
                            }
                            firstLine = false;
                            String[] split = StringUtils.split(original);
                            String filename = split[8];
                            String offset = split[7];
                            IFileEntry iFileEntry = iFileCache.getIFileEntry(filename, Long.parseLong(offset));
                            migrationOutput.append(filename).append(' ').append(offset).append(' ').append(iFileEntry.getNewOffset()).append(' ').append(iFileEntry.getTimestamp());
                            split[8] = filename + ".gz";
                            split[7] = "" + iFileEntry.getNewOffset();
                            cdxOutput.append(StringUtils.join(split, ' '));
                        } else {
                            logger.warn("Thread #{}: adapter.adaptLine of duplicate line '{}' in {} failed. Line ignored", threadNo, line, originalFilePath);
                            dedupEntriesFailed++;
                        }
                    } catch (AlreadyKnownMissingFileException e) {
                            logger.warn("Thread #{}: AlreadyKnownMissingFileException in duplicate line '{}' in file {}.", threadNo, line, originalFilePath, e);
                            dedupEntriesFailed++;
                    } catch (Exception e) {
                            logger.warn("Thread #{}: Error parsing duplicate line '{}' in  {}.", threadNo, line, originalFilePath, e);
                            dedupEntriesFailed++;
                    }
                }
            }
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
        logger.info("Thread #{}: DedupEntries found/failed for file '{}':{}/{}", 
                threadNo, originalFilePath, dedupEntriesFound, dedupEntriesFailed);
        logger.trace("Thread #{}: Cache size: {}", threadNo, iFileCache.getCurrentCachesize());
        return new byte[][] {migrationOutput.toString().getBytes(), cdxOutput.toString().getBytes()};
    }

    /**
     * Make a MD5 digest of the file, and append "filename##digest" to the file
     * given by the setting Util.UPDATED_FILENAME_MD5_FILEPATH .
     * @param fileToDigest The given file to digest
     * @throws DeeplyTroublingException
     */
    private static synchronized void writeMD5UpdatedFilename(File fileToDigest) throws DeeplyTroublingException {
        String md5;
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(fileToDigest);
            md5 = DigestUtils.md5Hex(inputStream);
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        String md5Filepath = Util.getProperties().getProperty(Util.UPDATED_FILENAME_MD5_FILEPATH);
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(md5Filepath, true)))) {
            writer.println(fileToDigest.getName() + "##" + md5);
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
    }

    @Override
    public void run() {
        while (!sharedQueue.isEmpty() && !isDead) {
            String filename = null;
            try {
                filename = sharedQueue.poll();
                if (filename != null) {
                    logger.info("Thread #{}: Processing {}. Left on queue: {}", threadNo, filename, sharedQueue.size());
                    processFile(filename);
                } else {
                    logger.info("Thread #{}: Sharedqueue should now be empty, and the processing is done. queue size: {}", threadNo, sharedQueue.size());
                }
            } catch (Exception e) {
                logger.warn("Thread #{}: Processing of {} threw an exception.", threadNo, filename, e);
            }
        }
        logger.info("Thread {} terminated naturally.", threadNo);
    }

    public static String valueOrNull(HeaderLine line) {
        if (line == null) {
            return null;
        } else {
            return line.value;
        }
    }

}
