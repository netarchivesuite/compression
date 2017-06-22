package dk.nationalbiblioteket.netarkivet.compression.precompression;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Multithreaded precompressor, based on http://stackoverflow.com/a/37862561
 */
public class PreCompressor {

    public static final String[] REQUIRED_PROPS = new String[] {Util.LOG, Util.IFILE_ROOT_DIR, Util.MD5_FILEPATH, Util.IFILE_DEPTH, Util.TEMP_DIR};

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
    private static String inputFile;

    private void fillQueue(String filelistFilename, String blacklistFilename) throws IOException, DeeplyTroublingException {
        List<String> blacklisted = readBlacklist(blacklistFilename);
        sharedQueue.addAll(Util.getFilteredList(filelistFilename, blacklisted));
        writeCompressionLog("Sharedqueue now filled with " + sharedQueue.size() + " elements"); 
    }
 
    private List<String> readBlacklist(String blacklistFilename) throws IOException, DeeplyTroublingException {
        if (blacklistFilename == null) {
            writeCompressionLog("No blacklistfile was given as argument!");
            return new ArrayList<String>();
        }
        List<String> blacklist = Util.getAllLines(blacklistFilename);
        writeCompressionLog("Read " + blacklist.size() + " entries from blacklistfile: " + blacklistFilename);
        return blacklist;
    }

    private void startConsumers() throws DeeplyTroublingException {
        int numberConsumers = Integer.parseInt(Util.getProperties().getProperty(Util.THREADS));
        writeCompressionLog("Starting PreCompressor on file " + inputFile + " using " + numberConsumers + " threads"); 
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new PrecompressionRunnable(sharedQueue, i)).start();
        }
        writeCompressionLog("All " + numberConsumers + " threads processing file " + inputFile + " started"); 
    }

    public static void main(String[] args) throws IOException, DeeplyTroublingException {
        String md5Filepath = Util.getProperties().getProperty(Util.MD5_FILEPATH);
        File tmpdir = (new File(Util.getProperties().getProperty(Util.TEMP_DIR)));
        tmpdir.mkdirs();
        FileUtils.cleanDirectory(tmpdir);
        (new File(md5Filepath)).getParentFile().mkdirs();
        PreCompressor preCompressor = new PreCompressor();
        inputFile = args[0];
        String blacklistFile = null;
        if (args.length > 1) {
            blacklistFile = args[1];
        }
        preCompressor.fillQueue(inputFile, blacklistFile);
        preCompressor.startConsumers();
    }

    private static synchronized void writeCompressionLog(String message) throws DeeplyTroublingException {
        String compressionLogPath = Util.getLogPath();
        String dateprefix = "[" +  new Date() + " ] ";
        message = dateprefix + message; 
        (new File(compressionLogPath)).getParentFile().mkdirs();
        Util.writeToFile(new File(compressionLogPath), message, 5, 1000L);
    }
}
