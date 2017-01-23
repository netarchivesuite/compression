package dk.nationalbiblioteket.netarkivet.compression.precompression;

import dk.nationalbiblioteket.netarkivet.compression.Util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Multithreaded precompressor, based on http://stackoverflow.com/a/37862561
 */
public class PreCompressor {

    public static final String[] REQUIRED_PROPS = new String[] {Util.LOG, Util.IFILE_ROOT_DIR, Util.MD5_FILEPATH, Util.IFILE_DEPTH, Util.TEMP_DIR};

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();

    private void fillQueue(String filelistFilename) throws IOException {
          sharedQueue.addAll(Files.readAllLines(Paths.get(filelistFilename)));
    }

    private void startConsumers() {
        int numberConsumers = Integer.parseInt(Util.getProperties().getProperty(Util.THREADS));
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new PrecompressionRunnable(sharedQueue, i)).start();
        }
    }

    public static void main(String[] args) throws IOException {
            PreCompressor preCompressor = new PreCompressor();
            String inputFile = args[0];
            preCompressor.fillQueue(inputFile);
            preCompressor.startConsumers();
    }

}
