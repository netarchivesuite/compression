package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
    public static Properties properties;


    private void fillQueue(String filelistFilename) throws IOException {
        this.filelistFilename = filelistFilename;
        List<String> blacklisted = readBlacklist();
        sharedQueue.addAll(Files.readAllLines(Paths.get(filelistFilename)).stream().filter(predicate(blacklisted)).collect(Collectors.toList()));
        logger.info("Loaded {} elements from file {} into sharedqueue", sharedQueue.size(), filelistFilename);
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
        metadatafileGenerator.fillQueue(inputFile);
        metadatafileGenerator.startConsumers();
    }
    
}
