package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates new metadatafiles from old metadatafiles - transforming cdx records to and adding new
 * record with dedup information.
 * 
 */
public class MetadatafileGenerator {
    public static final String CONFIG = "config";
    Logger logger = LoggerFactory.getLogger(MetadatafileGenerator.class);

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
    private String filelistFilename;

    private void fillQueue(String filelistFilename, String blacklistFilename) throws IOException {
        this.filelistFilename = filelistFilename;
        List<String> blacklisted = readBlacklist(blacklistFilename);
        sharedQueue.addAll(Util.getFilteredList(filelistFilename, blacklisted));
        logger.info("Loaded {} elements from file {} into sharedqueue", sharedQueue.size(), filelistFilename);
    }

    private List<String> readBlacklist(String blacklistFilename) throws IOException {
        if (blacklistFilename == null) {
            logger.info("No blacklistfile was given as argument!");
            return new ArrayList<String>();
        }
        List<String> blacklist = Util.getAllLines(blacklistFilename);
        logger.info("Read " + blacklist.size() + " entries from blacklistfile: " + blacklistFilename);
        return blacklist;
    }

    private void startConsumers() {
        int numberConsumers = Integer.parseInt(Util.getProperties().getProperty(Util.THREADS));
        logger.info("Starting {} MetadatafileGeneratorRunnable threads on file {} ", numberConsumers, filelistFilename);
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new MetadatafileGeneratorRunnable(sharedQueue, i)).start();
        }
        logger.info("Started all {} consumers on file {} ", numberConsumers, filelistFilename);
    }

    public static void main(String[] args) throws IOException {

        MetadatafileGenerator metadatafileGenerator = new MetadatafileGenerator();
        String inputFile = args[0];
        String blacklistFile = null;
        if (args.length > 1) {
            blacklistFile = args[1];
        }
        metadatafileGenerator.fillQueue(inputFile, blacklistFile);
        metadatafileGenerator.startConsumers();
    }
    
}
