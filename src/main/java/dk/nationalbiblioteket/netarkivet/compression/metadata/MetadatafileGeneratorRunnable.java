package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileCache;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileCacheFactory;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
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
    public MetadatafileGeneratorRunnable(BlockingQueue<String> sharedQueue, int threadNo) {
        System.setProperty("settings.harvester.harvesting.metadata.compression", "true");
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
    }

    /**
     * Takes a filename corresponding to an old-style uncompressed metadata file and creates a new
     * compressed metadata file with i) modified cdx records and ii) a new deduplication-info record.
     * Also renames the original file, replacing "metadata" in the name with "oldmetadata".
     * If Util.METADATA_DIR is not null then the filename is interpreted relative to that. Otherwise it
     * is relative to current working dir.
     *
     * If the file has already been reprocessed, then this method returns false
     *
     * @param filename
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
        logger.info("Processing {}.", inputFile.getAbsolutePath());
        final String replacementFilename = inputFile.getName().replace("metadata", "oldmetadata");
        final File dest = new File(inputFile.getParentFile(), replacementFilename);
        if (dest.exists()) {
            logger.info("Found output file {} so skipping.", dest.getAbsolutePath());
            return false;
        }
        if (!inputFile.exists()) {
            throw new NoSuchFileException("Input file " + inputFile.getAbsolutePath() + " not found, and file has not already been processed.");
        }
        String outputDir = properties.getProperty(Util.NMETADATA_DIR);
        if (outputDir == null) {
            outputDir = inputFile.getParentFile().getAbsolutePath();
        }
        final Path outputDirPath = Paths.get(outputDir);
        Files.createDirectories(outputDirPath);
        File cdxDir = Util.getCDXSubdir(inputFile.getName(), true);
        final String newMetadataFilename = Util.getNewMetadataFilename(filename);
        if (newMetadataFilename == null) {
            throw new WeirdFileException("Cannot work out how to generate new metadata file name from " + filename);
        }
        Path outputFilePath = outputDirPath.resolve(newMetadataFilename);
        if (filename.endsWith(".warc") || filename.endsWith(".warc.gz")) {
            processWarcfile(inputFile, outputFilePath.toFile(), cdxDir);
        } else if (filename.endsWith(".arc") ||filename.endsWith(".arc.gz")) {
            processArcfile(inputFile, outputFilePath.toFile(), cdxDir);
        } else {
            throw new WeirdFileException("Input metadata file is neither arc nor warc: " + filename);
        }
        inputFile.renameTo(dest);
        writeMD5UpdatedFilename(dest);
        writeMD5UpdatedFilename(outputFilePath.toFile());
        if (dest.length() >= outputFilePath.toFile().length()) {
            logger.warn("Very surprised to find that new metadata file {} is smaller than old metadata file {}.", outputFilePath, dest.getAbsolutePath());
        }
        logger.info("Finished done processing " + inputFile.getAbsolutePath());
        logger.trace(Util.getMemoryStats());
        return true;
    }

    private void processArcfile(File input, File output, File cdxDir) throws IOException, DeeplyTroublingException {
        logger.info("Processing ARC input file {} with outputfile {}.", input.getAbsolutePath(), output.getAbsolutePath());
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
                logger.debug("Processing {} from {}.", url, input.getAbsolutePath());
                Payload oldPayload = null;
                File oldPayloadFile = File.createTempFile("arcrecord", "txt");
                if (recordBase.hasPayload()) {
                    oldPayload = recordBase.getPayload();
                }
                if (oldPayload != null) {
                    try (InputStream oldPayloadIS = oldPayload.getInputStreamComplete()) {
                        Files.copy(oldPayloadIS, oldPayloadFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    }
                    if (url.contains("crawl.log")) {
                        String dedupURI = "metadata://crawl/index/deduplicationmigration?majorversion=0&minorversion=0";
                        String dedupCdxURI = "metadata://crawl/index/deduplicationcdx?majorversion=0&minorversion=0";
                        byte[][] dedupPayload = null;
                        try (InputStream oldPayloadIS = new FileInputStream(oldPayloadFile)) {
                            dedupPayload = getDedupPayload(oldPayloadIS);
                        }
                        writer.write(dedupURI, "text/plain", recordBase.getIpAddress(),
                                System.currentTimeMillis(), dedupPayload[0]);
                        logger.debug("Writing {} bytes to dedupmigration for {}.", dedupPayload[0].length, output.getAbsolutePath());
                        writer.write(dedupCdxURI, "text/plain", recordBase.getIpAddress(), System.currentTimeMillis(), dedupPayload[1]);
                        logger.debug("Writing {} bytes to dedupcdx for {}.", dedupPayload[1].length, output.getAbsolutePath());
                        final File dedupCdxFile = new File(cdxDir, input.getName() + ".cdx");
                        logger.debug("Creating dedup cdx file {}.", dedupCdxFile.getAbsolutePath());
                        FileUtils.writeByteArrayToFile(dedupCdxFile, dedupPayload[1]);
                    }
                    if (url.contains("index/cdx")) {
                        try (InputStream oldPayloadIS = new FileInputStream(oldPayloadFile)) {
                            payload = getUpdatedCdxPayload(oldPayloadIS);
                        }
                        url = url.replace(".arc", ".arc.gz");
                        logger.debug("Writing {} bytes to migrated cdx for {}.", payload.length, output.getAbsolutePath());
                        writer.write(url, recordBase.getContentTypeStr(),
                                recordBase.getIpAddress(), System.currentTimeMillis(), payload);
                    } else {
                        writer.writeTo(oldPayloadFile, url, recordBase.getContentTypeStr());
                    }
                } else {  //no payload
                    logger.debug("Writing empty record for {}.", url);
                    writer.writeTo(oldPayloadFile, url, recordBase.getContentTypeStr());
                }
                oldPayloadFile.delete();
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        logger.info("Finished processing ARC input file {}", input.getAbsolutePath());
    }

    private void processWarcfile(File input, File output, File cdxDir) throws IOException, DeeplyTroublingException {
        logger.info("Processing WARC input file {} with outputfile {}.", input.getAbsolutePath(), output.getAbsolutePath());
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
                            byte[][] dedupPayload = null;
                            try (InputStream oldPayloadIS = new FileInputStream(oldPayloadFile)) {
                                dedupPayload = getDedupPayload(oldPayloadIS);
                            }
                            String dedupURI = "metadata://crawl/index/deduplicationmigration?majorversion=0&minorversion=0";
                            writer.write(dedupURI, "text/plain", valueOrNull(hostIpLine), System.currentTimeMillis(), dedupPayload[0]);
                            logger.debug("Writing {} bytes to dedupmigration for {}.", dedupPayload[0].length, output.getAbsolutePath());
                            String dedupCdxURI = "metadata://crawl/index/dedupcdx?majorversion=0&minorversion=0";
                            writer.write(dedupCdxURI, "text/plain", valueOrNull(hostIpLine), System.currentTimeMillis(), dedupPayload[1]);
                            logger.debug("Writing {} bytes to dedupcdx for {}.", dedupPayload[1].length, output.getAbsolutePath());
                            if (dedupPayload[1].length > 0) {
                                FileUtils.writeByteArrayToFile(new File(cdxDir, input.getName() + ".cdx"), dedupPayload[1]);
                            }
                            writer.writeTo(oldPayloadFile, valueOrNull(uriLine), valueOrNull(contentTypeLine));
                        } else if (uriLine.value.contains("index/cdx")) {
                            uriLine.value = uriLine.value.replace("arc", "arc.gz");
                            try (InputStream oldPayloadIS = new FileInputStream(oldPayloadFile)) {
                                newPayloadBytes = getUpdatedCdxPayload(oldPayloadIS);
                            }
                            logger.debug("Writing {} bytes to migrated cdx for {}.", newPayloadBytes.length, output.getAbsolutePath());
                            writer.write(valueOrNull(uriLine),
                                    valueOrNull(contentTypeLine),
                                    valueOrNull(hostIpLine), System.currentTimeMillis(), newPayloadBytes);
                        } else {
                            writer.writeTo(oldPayloadFile, valueOrNull(uriLine), valueOrNull(contentTypeLine));
                        }
                    } else {
                        writer.writeTo(oldPayloadFile, valueOrNull(uriLine), valueOrNull(contentTypeLine));
                    }
                    oldPayloadFile.delete();
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        logger.info("Finished processing WARC input file {}", input.getAbsolutePath());
    }

    private byte[] getUpdatedCdxPayload(InputStream cdxPayloadIS) throws DeeplyTroublingException, FileNotFoundException {
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
                    //IFileCache iFileCache = IFileCacheFactory.getIFileCache(new IFileLoaderImpl());
                    IFileCache iFileCache = IFileCacheFactory.getIFileCache(new IFileTriLongLoaderImpl());
                    IFileEntry iFileEntry = iFileCache.getIFileEntry(file, oldOffset);
                    captureSearchResult.setOffset(iFileEntry.getNewOffset());
                    captureSearchResult.setFile(file + ".gz");
                    line = cdxFormat.serializeResult(captureSearchResult);
                    if (!firstLine) {
                        sb.append("\n");
                    }
                    sb.append(line);
                    firstLine = false;
                } catch (Exception e) {
                    logger.warn("Error processing '" + line + "'", e);
                }
            }
            return sb.toString().getBytes();
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        } finally {
            IOUtils.closeQuietly(br);
        }
    }

    private byte[][] getDedupPayload(InputStream crawllogPayloadIS) throws DeeplyTroublingException {
        //final IFileCache iFileCache = IFileCacheFactory.getIFileCache(new IFileLoaderImpl());
        final IFileCache iFileCache = IFileCacheFactory.getIFileCache(new IFileTriLongLoaderImpl());
        StringBuffer migrationOutput = new StringBuffer();
        StringBuffer cdxOutput = new StringBuffer();
        DeduplicateToCDXAdapter adapter = new DeduplicateToCDXAdapter();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(crawllogPayloadIS))) {
            boolean firstLine = true;
            String line;
            while ((line = br.readLine()) != null && line.trim().length() > 0) {
                if (line.contains("duplicate:")) {
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
                        }
                    } catch (Exception e) {
                        //There are known examples of bad lines in crawl logs.
                        logger.warn("Error parsing '{}'.", line, e);
                    }
                }
            }
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
        logger.trace("Cache size: {}", iFileCache.getCurrentCachesize());
        return new byte[][] {migrationOutput.toString().getBytes(), cdxOutput.toString().getBytes()};
    }

    private static synchronized void writeMD5UpdatedFilename(File gzipFile) throws DeeplyTroublingException {
        String md5;
        FileInputStream gzipfileInputStream = null;
        try {
            gzipfileInputStream = new FileInputStream(gzipFile);
            md5 = DigestUtils.md5Hex(gzipfileInputStream);
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        } finally {
            IOUtils.closeQuietly(gzipfileInputStream);
        }
        String md5Filepath = Util.getProperties().getProperty(Util.UPDATED_FILENAME_MD5_FILEPATH);
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(md5Filepath, true)))) {
            writer.println(gzipFile.getName() + "##" + md5);
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
                    logger.info("Processing {} with thread {}. Left on queue: {}", filename, threadNo, sharedQueue.size());
                    processFile(filename);
                } else {
                    logger.info("Sharedqueue should now be empty, and the processing of thread {} is done. queue size:{}", threadNo, sharedQueue.size());
                }
            } catch (Exception e) {
                logger.warn("Processing of {} threw an exception.", filename, e);
            }
        }
        logger.info("Thread {} died of natural causes.", threadNo);
    }

    public static String valueOrNull(HeaderLine line) {
        if (line == null) {
            return null;
        } else {
            return line.value;
        }
    }

}
