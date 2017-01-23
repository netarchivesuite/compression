package dk.nationalbiblioteket.netarkivet.compression.compression;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.jwat.tools.tasks.compress.CompressFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by csr on 1/11/17.
 */
public class Compressor{

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();



    private void fillQueue(String filelistFilename) throws IOException {
          sharedQueue.addAll(Files.readAllLines(Paths.get(filelistFilename)));
    }

    private void startConsumers() {
        int numberConsumers = Integer.parseInt(Util.getProperties().getProperty(Util.THREADS));
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new CompressorRunnable(sharedQueue, i)).start();
        }
    }

    public static void main(String[] args) throws IOException {
            Compressor compressor = new Compressor();
            String inputFile = args[0];
            compressor.fillQueue(inputFile);
            compressor.startConsumers();
    }


}
