package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriter;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriterArc;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriterWarc;
import dk.netarkivet.wayback.batch.DeduplicateToCDXAdapter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.archive.util.io.RuntimeIOException;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    public MetadatafileGeneratorRunnable(BlockingQueue<String> sharedQueue, int threadNo) {
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
    }

    /**
     * Takes a filename corresponding to an old-style uncompressed metadata file and creates a new
     * compressed metadata file with i) modified cdx records and ii) a new deduplication-info record.
     * If Util.METADATA_DIR is not null then the filename is interpreted relative to that. Otherwise it
     * is relative to current working dir.
     * @param filename
     */
    void processFile(String filename) throws IOException, WeirdFileException {
        Properties properties = Util.getProperties();
        String inputPath = properties.getProperty(Util.METADATA_DIR);
        File inputFile;
        if (inputPath != null)  {
            inputFile = new File(inputPath, filename);
        } else {
            inputFile = new File(filename);
        }
        if (!inputFile.exists()) {
            throw new NoSuchFileException("No such file: " + inputFile.getAbsolutePath());
        }
        final Path outputDirPath = Paths.get(properties.getProperty(Util.NMETADATA_DIR));
        Files.createDirectories(outputDirPath);
        Path outputFilePath = outputDirPath.resolve(Util.getNewMetadataFilename(filename));
        //Files.createFile(outputFilePath);
        if (filename.endsWith(".warc")) {
             processWarcfile(inputFile, outputFilePath.toFile());
        } else if (filename.endsWith(".arc")) {
             processArcfile(inputFile, outputFilePath.toFile());
        } else {
            throw new WeirdFileException("Input metadata file is neither arc nor ward: " + filename);
        }
    }

    private void processArcfile(File input, File output) throws IOException {
        MetadataFileWriter writer = MetadataFileWriterArc.createWriter(output);
        InputStream is = new FileInputStream(input);
        ArcReader reader = ArcReaderFactory.getReader(is);
        final Iterator<ArcRecordBase> iterator = reader.iterator();
        try {
            while (iterator.hasNext()) {
                ArcRecordBase recordBase = iterator.next();
                byte[] payload = new byte[]{};
                if (recordBase.hasPayload()) {
                    payload = IOUtils.toByteArray(recordBase.getPayloadContent());
                }
                String url = recordBase.getUrlStr();
                if (url.contains("crawl.log")) {
                    String dedupURI = "metadata://crawl/index/deduplicationcdx?majorversion=0&minorversion=0";
                    byte[] dedupPayload = getDedupPayload(payload);
                    writer.write(dedupURI, "text/plain", recordBase.getIpAddress(),
                    System.currentTimeMillis(), dedupPayload);
                } else if (url.contains("index/cdx")) {
                    payload = getUpdatedCdxPayload(payload);
                    url = url.replace(".arc", ".arc.gz");
                }
                writer.write(url, recordBase.getContentTypeStr(),
                        recordBase.getIpAddress(), System.currentTimeMillis(), payload);
            }
        } catch (CDXFormatException e) {
            throw new RuntimeException(e);
        }
        finally {
            writer.close();
        }
    }

    private void processWarcfile(File input, File output) throws IOException {
        MetadataFileWriter writer = MetadataFileWriterWarc.createWriter(output);
        InputStream is = new FileInputStream(input);
        WarcReader reader = WarcReaderFactory.getReader(is);
        final Iterator<WarcRecord> iterator = reader.iterator();
        try {
            while (iterator.hasNext()) {
                WarcRecord record = iterator.next();
                if (record.getHeader(WarcConstants.FN_WARC_TYPE).value.equals("warcinfo")) {
                    ANVLRecord infoPayload = new ANVLRecord();
                    infoPayload.addLabelValue("replaces", record.getHeader(WarcConstants.FN_WARC_FILENAME).value);
                    infoPayload.addValue(IOUtils.toString(record.getPayloadContent()));
                    ((MetadataFileWriterWarc) writer).insertInfoRecord(infoPayload);

                } else if (record.getHeader(WarcConstants.FN_WARC_TYPE).value.equals("resource")) {
                    final HeaderLine uriLine = record.getHeader(WarcConstants.FN_WARC_TARGET_URI);
                    final HeaderLine contentTypeLine = record.getHeader(WarcConstants.FN_CONTENT_TYPE);
                    final HeaderLine hostIpLine = record.getHeader(WarcConstants.FN_WARC_IP_ADDRESS);
                    byte[] payloadBytes = new byte[]{};

                    if (record.hasPayload()) {
                        Payload payload = record.getPayload();
                        payloadBytes = IOUtils.toByteArray(payload.getInputStreamComplete());
                    }
                    //Note that in the case of the crawl log we add a new record, whereas in the case of
                    //the cdx records we alter the record.
                    if (uriLine.value.contains("crawl.log")) {
                        //Ideally would like to include harvestid, harvestnum, jobid in following uri.
                        String dedupURI = "metadata://crawl/index/deduplicationcdx?majorversion=0&minorversion=0";
                        byte[] dedupPayload = getDedupPayload(payloadBytes);
                        writer.write(dedupURI, "text/plain", valueOrNull(hostIpLine), System.currentTimeMillis(), dedupPayload);
                    } else if (uriLine.value.contains("index/cdx")) {
                        uriLine.value = uriLine.value.replace("arc", "arc.gz");
                        payloadBytes = getUpdatedCdxPayload(payloadBytes);
                    }
                    writer.write(valueOrNull(uriLine),
                            valueOrNull(contentTypeLine),
                            valueOrNull(hostIpLine), System.currentTimeMillis(), payloadBytes);
                }
            }
        } catch (CDXFormatException e) {
            throw new RuntimeException(e);
        } finally {
            writer.close();
        }
    }

    private byte[] getUpdatedCdxPayload(byte[] cdxPayload) throws CDXFormatException, FileNotFoundException {
        String cdxSpec = " CDX A r b m S g V k";
        CDXFormat cdxFormat = new CDXFormat(cdxSpec);
        String[] payload = new String(cdxPayload).split("\\r\\n|\\n|\\r");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < payload.length; i++) {
            String line = payload[i];
            CaptureSearchResult captureSearchResult = null;
            try {
                captureSearchResult = cdxFormat.parseResult(line);
            } catch (CDXFormatException e) {
                throw new RuntimeIOException("Cannot parse " + line);
            }
            String file = captureSearchResult.getFile();
            long oldOffset = captureSearchResult.getOffset();
            IFileEntry iFileEntry = IFileCacheImpl.getIFileCacheImpl().getIFileEntry(file, oldOffset);
            captureSearchResult.setOffset(iFileEntry.getNewOffset());
            captureSearchResult.setFile(file + ".gz");
            line = cdxFormat.serializeResult(captureSearchResult);


            sb.append(line);
            if (i != payload.length -1) {
                sb.append("\n");
            }
        }
        return sb.toString().getBytes();
    }

    private byte[] getDedupPayload(byte[] crawllogPayload) throws FileNotFoundException {
        String[] input = new String(crawllogPayload).split("\\r\\n|\\n|\\r");
        StringBuffer output = new StringBuffer();
        DeduplicateToCDXAdapter adapter = new DeduplicateToCDXAdapter();
        for (String line: input) {
            if (line.contains("duplicate:")) {
                String original = adapter.adaptLine(line);
                String[] split = StringUtils.split(original);
                String filename = split[8];
                String offset = split[7];
                IFileEntry iFileEntry = IFileCacheImpl.getIFileCacheImpl().getIFileEntry(filename, Long.parseLong(offset));
                split[8] = filename + ".gz";
                split[7] = "" + iFileEntry.getNewOffset();
                output.append(StringUtils.join(split, ' '));
                output.append("\n");
            }
        }
        return output.toString().getBytes();
    }



    @Override
    public void run() {
         while (!sharedQueue.isEmpty() && !isDead) {
             String filename = null;
             try {
                 filename = sharedQueue.take();
                 processFile(filename);
             } catch (InterruptedException e) {
                 throw new RuntimeException(e);
             } catch (WeirdFileException | IOException e) {
                 isDead = true;
                 throw new RuntimeException(e);
             }
         }
    }
    public static String valueOrNull(HeaderLine line) {
        if (line == null) {
            return null;
        } else {
            return line.value;
        }
    }

}
