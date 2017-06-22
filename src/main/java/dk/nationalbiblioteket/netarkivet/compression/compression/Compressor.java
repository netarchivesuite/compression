package dk.nationalbiblioteket.netarkivet.compression.compression;

import dk.nationalbiblioteket.netarkivet.compression.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by csr on 1/11/17.
 */
public class Compressor{

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
    String filelistFilename;

    private void fillQueue(String filelistFilename, String blacklistFilename) throws IOException {
        this.filelistFilename = filelistFilename;
        List<String> blacklisted = readBlacklist(filelistFilename);
        sharedQueue.addAll(Files.readAllLines(Paths.get(filelistFilename)).stream().filter(predicate(blacklisted)).collect(Collectors.toList()));
        writeCompressionLog("Sharedqueue now filled with " + sharedQueue.size() + " elements"); 
    }

    private Predicate<String> predicate(List<String> blacklisted) {
        return p -> !p.isEmpty() && !blacklisted.contains(p);
    }

    private List<String> readBlacklist(String blacklistFilename) throws IOException {
        //ClassLoader classLoader = getClass().getClassLoader();
        //String blacklistFilename = "blacklisted_metadatafiles.txt"; // Located physically in src/main/resources/blacklisted_metadatafiles.txt
        //File file = new File(classLoader.getResource(blacklistFilename).getFile());
        if (blacklistFilename == null) {
            writeCompressionLog("No blacklistfile was given as argument!");
            return new ArrayList<String>();
        }
        List<String> blacklist = Files.readAllLines(Paths.get(blacklistFilename));
        writeCompressionLog("Read " + blacklist.size() + " entries from blacklistfile: " + blacklistFilename);
        return blacklist;
    }


    private void startConsumers() {
        int numberConsumers = Integer.parseInt(Util.getProperties().getProperty(Util.THREADS));
        writeCompressionLog("Starting compressor on file " + filelistFilename + " using " + numberConsumers + " threads"); 
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new CompressorRunnable(sharedQueue, i)).start();
        }
        writeCompressionLog("All " + numberConsumers + " threads processing file " + filelistFilename + " started"); 
    }

    public static void main(String[] args) throws IOException {
        String md5Filepath = Util.getProperties().getProperty(Util.MD5_FILEPATH);
        File md5File = new File(md5Filepath);
        if (!md5File.exists() || md5File.isDirectory()) {
            System.out.println("No such file " + md5File.getAbsolutePath());
            System.exit(1);
        }
        Compressor compressor = new Compressor();
        String inputFile = args[0];
        String blacklistFile = null;
        if (args.length > 1) {
            blacklistFile = args[1];
        }
        compressor.fillQueue(inputFile, blacklistFile);
        compressor.startConsumers();
    }
    
    private static synchronized void writeCompressionLog(String message) {
        String compressionLogPath = Util.getLogPath();
        String dateprefix = "[" +  new Date() + "] ";
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(compressionLogPath, true)))) {
            writer.println(dateprefix + message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
