package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;

/**
 * Multithreaded precompressor, based on http://stackoverflow.com/a/37862561
 */
public class PreCompressor {

    public static final String CONFIG = "config";
    public static final String LOG = "LOG";
    public static final String OUTPUT_ROOT_DIR = "OUTPUT_ROOT_DIR";
    public static final String MD5_FILEPATH = "MD5_FILEPATH";
    public static final String DEPTH = "DEPTH";
    public static final String TEMP_DIR = "TEMP_DIR";
    public static final String[] REQUIRED_PROPS = new String[] {LOG, OUTPUT_ROOT_DIR, MD5_FILEPATH, DEPTH, TEMP_DIR};

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
    public static Properties properties;


    private void fillQueue(String filelistFilename) throws IOException {
          sharedQueue.addAll(Files.readAllLines(Paths.get(filelistFilename)));
    }

    private void startConsumers() {
        int numberConsumers = 10;
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new Consumer(sharedQueue, i)).start();
        }
    }

    public static void main(String[] args) throws IOException {
            PreCompressor preCompressor = new PreCompressor();
            String inputFile = args[0];
            String configFile = System.getProperty(CONFIG);
            properties = new Properties();
            properties.load(new FileInputStream(new File(configFile)));
            preCompressor.fillQueue(inputFile);
            preCompressor.startConsumers();
    }

}
