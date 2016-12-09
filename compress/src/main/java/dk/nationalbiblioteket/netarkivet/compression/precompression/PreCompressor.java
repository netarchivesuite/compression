package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;

/**
 * Multithreaded precompressor, based on http://stackoverflow.com/a/37862561
 */
public class PreCompressor {

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
    public static Map<String, String> propertiesMap;


    static String logfile;

    static void exit(String message, String filename,  int returnValue) {
        String output = (new Date()).toString() + " " + filename  + " " + message;
        if (logfile != null) {

        } else {
            System.err.println(output);
        }
        System.exit(returnValue);
    }



    private static Map<String, String> extractConfig(String inputFile, String configFile) throws IOException {
        Map<String, String> propertiesMap = new HashMap<String, String>();
        for (Object lineObject: FileUtils.readLines(new File(inputFile))) {
            String line = (String) lineObject;
            if (line.contains("=")) {
                String[] keyValue = line.split("=");
                propertiesMap.put(keyValue[0], keyValue[1]);
            }
        }
        return propertiesMap;
    }

    private void fillQueue(String filelistFilename) {

    }

    private void startConsumers() {
        int numberConsumers = 10;
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new Consumer(sharedQueue, i)).start();
        }
    }

    public static void main(String[] args) {
        PreCompressor preCompressor = new PreCompressor();
        String inputFile = args[1];
        String configFile = System.getProperty("config");
        try {
            propertiesMap = extractConfig(inputFile, configFile);
        } catch (IOException e) {
            exit("Error reading " + configFile, inputFile, 21);
        }
        logfile = propertiesMap.get("LOG");
        preCompressor.fillQueue(inputFile);
        preCompressor.startConsumers();
    }

}
