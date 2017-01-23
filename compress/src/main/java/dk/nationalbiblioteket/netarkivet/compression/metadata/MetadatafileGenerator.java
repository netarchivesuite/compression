package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.precompression.PrecompressionRunnable;
import dk.netarkivet.common.utils.Settings;
import dk.netarkivet.harvester.HarvesterSettings;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Generates new metadatafiles from old metadatafiles - transforming cdx records to and adding new
 * record with dedup information.
 */
public class MetadatafileGenerator {
    public static final String CONFIG = "config";

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
    public static Properties properties;


    private void fillQueue(String filelistFilename) throws IOException {
        sharedQueue.addAll(Files.readAllLines(Paths.get(filelistFilename)));
    }

    private void startConsumers() {
        int numberConsumers = Integer.parseInt(Util.getProperties().getProperty(Util.THREADS));
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new MetadatafileGeneratorRunnable(sharedQueue, i)).start();
        }
    }

    public static void main(String[] args) throws IOException {

        MetadatafileGenerator metadatafileGenerator = new MetadatafileGenerator();
        String inputFile = args[0];
        metadatafileGenerator.fillQueue(inputFile);
        metadatafileGenerator.startConsumers();
    }

}
