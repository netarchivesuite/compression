package dk.nationalbiblioteket.netarkivet.compression.compression;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.jwat.arc.ArcHeader;
import org.jwat.arc.ArcReader;
import org.jwat.archive.ArchiveRecordParserCallback;
import org.jwat.tools.tasks.compress.CompressFile;
import org.jwat.tools.tasks.compress.CompressOptions;
import org.jwat.tools.tasks.compress.CompressResult;
import org.jwat.warc.WarcHeader;
import org.jwat.warc.WarcReader;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;

/**
 * Created by csr on 1/11/17.
 */
public class CompressorRunnable extends CompressFile implements Runnable {

    private final int defaultRecordHeaderMaxSize = 1024 * 1024;
    private final int defaultPayloadHeaderMaxSize = 1024 * 1024;

    private boolean isDead = false;
    private final BlockingQueue<String> sharedQueue;
    private int threadNo;
    static {
        Logger logger = Logger.getLogger("org.archive.wayback.resourcestore.indexer");
        logger.setLevel(Level.WARNING);
    }

    private static synchronized void writeCompressionLog(String message, int threadNo) {
        String compressionLogPath = Util.getLogPath();
        String dateprefix = "[" +  new Date() + "(thread: "+ threadNo  +") ] ";
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(compressionLogPath, true)))) {
            writer.println(dateprefix + message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compresses the file. 
     * Throws an exception if it cannot be compressed for some other reason.
     * @param filename
     * @return true if the file is compressed, false if it cannot be compressed for a well-understood reason.
     * @throws DeeplyTroublingException
     * @throws WeirdFileException
     * @throws IOException
     */
    public boolean compress(String filename) throws DeeplyTroublingException, WeirdFileException, IOException {
        File inputFile = new File(filename);
        if (!inputFile.exists() || !inputFile.isFile()) {
            writeCompressionLog(inputFile.getAbsolutePath() + " was not compressed. File does not exist or represents a directory", threadNo);
            return false;
        }
        if (inputFile.length() == 0) {
            writeCompressionLog(inputFile.getAbsolutePath() + " was not compressed. Is zero size file.", threadNo);
            return false;
        }
        File gzipFile = doCompression(inputFile);
        validateMD5(gzipFile);
        String osName = System.getProperty("os.name");
        if (osName.contains("Windows")){
             if (gzipFile.getName().contains("metadata")) {
                 String newName = gzipFile.getName().replace("metadata", "oldmetadata"); 
                 File newFile = new File(gzipFile.getParentFile(), newName);
                 writeCompressionLog("Trying to rename file " + gzipFile.getAbsolutePath() + " as " +  newFile.getAbsolutePath() + " on " +  osName,threadNo);
                 writeRename(gzipFile, newFile);
                 Runtime.getRuntime().exec("cmd \\c rename \"" + gzipFile.getAbsolutePath() + "\" " + newFile.getName());
             }
        }

        boolean dryrun = Boolean.parseBoolean(Util.getProperties().getProperty(Util.DRYRUN));
        if (!dryrun) {
            boolean isWritable = inputFile.setWritable(true);
            if (!isWritable) {
                writeCompressionLog(inputFile.getAbsolutePath() + " not set to writable. Unknown reason", threadNo);
            }
            System.gc(); // FIXME What is this good for??
            boolean deleted = inputFile.delete();
            if (!deleted) {
                writeCompressionLog("WARNING: Trying to delete file '" 
                        + inputFile.getAbsolutePath() + "', but failed. Trying with deleteOnExit()", threadNo);
                inputFile.deleteOnExit();
            }
        } else {
            writeCompressionLog("Running in DRYRUN mode. No deletion of inputfile " +  inputFile.getAbsolutePath(), threadNo);
        }
        return true;
    }

    private static synchronized void writeRename(File oldFile, File newFile) throws DeeplyTroublingException {
           File renameFile = new File("rename.bat");
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(renameFile, true)))) {
            writer.println("rename \"" + oldFile.getAbsolutePath() + "\" " + newFile.getName());
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
    }

    private static void validateMD5(File gzipFile) throws DeeplyTroublingException, IOException {
        String md5;
        InputStream gzipFileStream = null;
        try {
            gzipFileStream = new FileInputStream(gzipFile);
            md5 = DigestUtils.md5Hex(gzipFileStream);
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        } finally {
            IOUtils.closeQuietly(gzipFileStream);
        }
        String md5Filepath = Util.getProperties().getProperty(Util.MD5_FILEPATH);
        LineIterator lineIterator = FileUtils.lineIterator(new File(md5Filepath));
        try {
            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                if (line.startsWith(gzipFile.getName())) {
                    String md5Expected = StringUtils.splitByWholeSeparator(line, "##")[1].trim();
                    if (md5.equals(md5Expected)) {
                        return;
                    } else {
                        throw new DeeplyTroublingException("Wrong checksum for " + gzipFile);
                    }
                }
            }
        } finally {
            lineIterator.close();
        }
    }


    public CompressorRunnable(BlockingQueue<String> sharedQueue, int threadNo) {
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
    }

    private int getIntProperty(String propertyKey, int defaultPropertyValue) {
        String propertyValue = Util.getProperties().getProperty(propertyKey);
        if (propertyValue == null) {
            return defaultPropertyValue;
        } else {
            int propertyValueInt = Integer.decode(propertyValue);
            return propertyValueInt;
        }
    }

    private File doCompression(File inputFile) throws WeirdFileException {
    	final int recordHeaderMaxSize = getIntProperty(Util.RECORD_HEADER_MAXSIZE, defaultRecordHeaderMaxSize);
        final int payloadHeaderMaxSize = getIntProperty(Util.PAYLOAD_HEADER_MAXSIZE, defaultPayloadHeaderMaxSize);
        File gzipFile = getOutputGzipFile(inputFile);
        gzipFile.getParentFile().mkdirs();
        CompressOptions compressOptions = new CompressOptions();
        compressOptions.dstPath = gzipFile.getParentFile();
        compressOptions.bTwopass = true;
        compressOptions.compressionLevel = Integer.parseInt(Util.COMPRESSION_LEVEL);
        compressOptions.recordHeaderMaxSize = recordHeaderMaxSize;
        compressOptions.payloadHeaderMaxSize = payloadHeaderMaxSize;
        if ("23312-55-20071125023519-00403-kb-prod-har-002.kb.dk.arc".equalsIgnoreCase(inputFile.getName())) {
            compressOptions.arpCallback = new ArchiveRecordParserCallback() {
    			@Override
    			public void arcParsedRecordHeader(ArcReader reader, long startOffset, ArcHeader header) {
    				if (startOffset == 81984113 && header.archiveLength == 14493) {
    					System.out.println(Long.toHexString(startOffset) + " " + startOffset + " " + header.archiveLength + " " + header.archiveLengthStr);
    					header.archiveLength = 8192L;
    					header.archiveLengthStr = "8192";
    				}
    			}
    			@Override
    			public void warcParsedRecordHeader(WarcReader reader, long startOffset, WarcHeader header) {
    			}
    		};
        }
        CompressResult result = this.compressFile(inputFile, compressOptions);
        if (!gzipFile.exists()) {
            throw new WeirdFileException("Compressed file " + gzipFile.getAbsolutePath() + " was not created.", result.getThrowable());
        } else {
            return gzipFile;
        }
    }

    private File getOutputGzipFile(File inputFile) {
        final String outputDirString = Util.getProperties().getProperty(Util.OUTPUT_DIR);
        File outputDir = null;
        if (outputDirString == null || outputDirString.trim().length() == 0) {
            outputDir = inputFile.getParentFile();
        } else {
            outputDir = new File(outputDirString);
        }
        return new File(outputDir, inputFile.getName() + ".gz");
    }

    @Override
    public void run() {
        while (!sharedQueue.isEmpty() && !isDead) {
            String filename = null;
            try {
                writeCompressionLog("Files left in the sharedQueue: " + sharedQueue.size(), threadNo);
                filename = sharedQueue.poll();
                if (filename != null){
                    writeCompressionLog("Compression of file  " + filename + " started. Now files left in queue: " + sharedQueue.size(), threadNo);
                    if (compress(filename)) {
                        writeCompressionLog("Compressed " + filename + " to " + getOutputGzipFile(new File(filename)).getAbsolutePath(), threadNo);
                    }
                } else {
                    writeCompressionLog("Queue seems to be empty now. Nothing more to do.", threadNo);
                }
            } catch (Exception e) {
                isDead = true;
                writeCompressionLog("Failed to compress " + filename + ".Cause: " + e, threadNo);
                throw new RuntimeException(e);
            }
        }
    }
}
