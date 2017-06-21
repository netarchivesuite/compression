package dk.nationalbiblioteket.netarkivet.compression.precompression;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Multithreaded precompressor, based on http://stackoverflow.com/a/37862561
 */
public class PreCompressor {

    public static final String[] REQUIRED_PROPS = new String[] {Util.LOG, Util.IFILE_ROOT_DIR, Util.MD5_FILEPATH, Util.IFILE_DEPTH, Util.TEMP_DIR};

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
    private static String inputFile;

    private void fillQueue(String filelistFilename) throws IOException, DeeplyTroublingException {
        List<String> blacklisted = readBlacklist();
        sharedQueue.addAll(Files.readAllLines(Paths.get(filelistFilename)).stream().filter(predicate(blacklisted)).collect(Collectors.toList()));
        writeCompressionLog("Sharedqueue now filled with " + sharedQueue.size() + " elements"); 
    }
    
    private Predicate<String> predicate(List<String> blacklisted) {
        return p -> !p.isEmpty() && !blacklisted.contains(p);
    }

    private List<String> readBlacklist() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        String blacklistFilename = "blacklisted_metadatafiles.txt"; // Located physically in src/main/resources/blacklisted_metadatafiles.txt
        File file = new File(classLoader.getResource(blacklistFilename).getFile());
        return Files.readAllLines(Paths.get(file.getAbsolutePath()));
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
        preCompressor.fillQueue(inputFile);
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
