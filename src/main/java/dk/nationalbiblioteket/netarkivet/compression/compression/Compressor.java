package dk.nationalbiblioteket.netarkivet.compression.compression;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.apache.commons.lang3.StringUtils;
import org.jwat.tools.tasks.compress.CompressFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Created by csr on 1/11/17.
 */
public class Compressor{

    BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();

    private void fillQueue(String filelistFilename) throws IOException {
          sharedQueue.addAll(Files.readAllLines(Paths.get(filelistFilename)).stream().filter(StringUtils::isNotBlank).collect(Collectors.toList()));
    }

    private void startConsumers() {
        int numberConsumers = Integer.parseInt(Util.getProperties().getProperty(Util.THREADS));
        for (int i = 0; i < numberConsumers; i++) {
            new Thread(new CompressorRunnable(sharedQueue, i)).start();
        }
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
            compressor.fillQueue(inputFile);
            compressor.startConsumers();
    }


}
