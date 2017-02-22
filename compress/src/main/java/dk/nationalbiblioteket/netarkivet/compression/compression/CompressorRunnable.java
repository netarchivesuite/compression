package dk.nationalbiblioteket.netarkivet.compression.compression;

import dk.nationalbiblioteket.netarkivet.compression.FatalException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.jwat.tools.tasks.compress.CompressFile;
import org.jwat.tools.tasks.compress.CompressOptions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by csr on 1/11/17.
 */
public class CompressorRunnable extends CompressFile implements Runnable {

    private boolean isDead = false;
    private final BlockingQueue<String> sharedQueue;
    private int threadNo;
    static {
        Logger logger = Logger.getLogger("org.archive.wayback.resourcestore.indexer");
        logger.setLevel(Level.WARNING);
    }

    private static synchronized void writeCompressionLog(String message) {
        String compressionLogPath = Util.getProperties().getProperty(Util.LOG);
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(compressionLogPath, true)))) {
            writer.println(message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void compress(String filename) throws FatalException, WeirdFileException, IOException {
        File inputFile = new File(filename);
        if (inputFile.length() == 0) {
            writeCompressionLog(inputFile.getAbsolutePath() + " not compressed. Zero size file.");
            return;
        }
        File gzipFile = doCompression(inputFile);
        validateMD5(gzipFile);
        if (System.getProperty("os.name").contains("Windows")){
             if (gzipFile.getName().contains("metadata")) {
                 String newName = gzipFile.getName().replace("metadata", "oldmetadata");
                 File newFile = new File(gzipFile.getParentFile(), newName);
                 writeRename(gzipFile, newFile);
                 //Files.move(gzipFile.toPath(), newFile.toPath());
                 Runtime.getRuntime().exec("cmd \\c rename \"" + gzipFile.getAbsolutePath() + "\" " + newFile.getName());
             }
        }
        inputFile.setWritable(true);
        System.gc();
        inputFile.delete();
        if (inputFile.exists()) {
            inputFile.deleteOnExit();
        }
    }

    private static synchronized void writeRename(File oldFile, File newFile) throws FatalException {
           File renameFile = new File("rename.bat");
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(renameFile, true)))) {
            writer.println("rename \"" + oldFile.getAbsolutePath() + "\" " + newFile.getName());
        } catch (IOException e) {
            throw new FatalException(e);
        }
    }

    private static void validateMD5(File gzipFile) throws FatalException, IOException {
        String md5;
        try {
            md5 = DigestUtils.md5Hex(new FileInputStream(gzipFile));
        } catch (IOException e) {
            throw new FatalException(e);
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
                        throw new FatalException("Wrong checksum for " + gzipFile);
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

    private File doCompression(File inputFile) throws WeirdFileException {
        CompressOptions compressOptions = new CompressOptions();
        compressOptions.dstPath = inputFile.getParentFile();
        compressOptions.bTwopass = true;
        compressOptions.compressionLevel = Integer.parseInt(Util.COMPRESSION_LEVEL);
        this.compressFile(inputFile, compressOptions);
        File gzipFile = new File (inputFile.getParentFile(), inputFile.getName() + ".gz");
        if (!gzipFile.exists()) {
            throw new WeirdFileException("Compressed file " + gzipFile.getAbsolutePath() + " not created.");
        } else {
            return gzipFile;
        }
    }


    @Override
    public void run() {
        while (!sharedQueue.isEmpty() && !isDead) {
            String filename = null;
            try {
                filename = sharedQueue.take();
                compress(filename);
                writeCompressionLog("Compressed " + filename + " to " + filename + ".gz");
            } catch (Exception e) {
                isDead = true;
                writeCompressionLog("Failed to compress " + filename + " " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }
}