package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.precompression.FatalException;
import dk.nationalbiblioteket.netarkivet.compression.precompression.WeirdFileException;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * Created by csr on 1/4/17.
 */
public class MetadatafileGeneratorRunnable implements Runnable {

    private final BlockingQueue<String> sharedQueue;
    private int threadNo;
    private static boolean isDead = false;

    public MetadatafileGeneratorRunnable(BlockingQueue<String> sharedQueue, int threadNo) {
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
    }

    /**
     * Takes a filename corresponding to an old-style uncompressed metadata file and creates a new
     * compressed metadata file with i) modified cdx records and ii) a new deduplication-info record.
     * If Util.METADATA_DIR is not null then the filename is interpreted relative to that. Otherwise it
     * is relative to current working dir.
     * @param filename
     */
    void processFile(String filename) throws IOException, WeirdFileException {
        Properties properties = Util.getProperties();
        String inputPath = properties.getProperty(Util.METADATA_DIR);
        File inputFile;
        if (inputPath != null)  {
            inputFile = new File(inputPath, filename);
        } else {
            inputFile = new File(filename);
        }
        if (!inputFile.exists()) {
            throw new NoSuchFileException("No such file: " + inputFile.getAbsolutePath());
        }
        final Path outputDirPath = Paths.get(properties.getProperty(Util.NMETADATA_DIR));
        Files.createDirectories(outputDirPath);
        Path outputFilePath = outputDirPath.resolve(inputFile.getName() + ".gz");
        //Files.createFile(outputFilePath);
        if (filename.endsWith(".warc")) {
             processWarcfile(inputFile, outputFilePath.toFile());
        } else if (filename.endsWith(".arc")) {
             processArcfile(inputFile, outputFilePath.toFile());
        } else {
            throw new WeirdFileException("Input metadata file is neither arc nor ward: " + filename);
        }
    }

    private void processWarcfile(File input, File output) {
        throw new RuntimeException("Not implemented");
    }

    private void processArcfile(File input, File output) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void run() {
         while (!sharedQueue.isEmpty() && !isDead) {
             String filename = null;
             try {
                 filename = sharedQueue.take();
                 processFile(filename);
             } catch (InterruptedException e) {
                 throw new RuntimeException(e);
             } catch (WeirdFileException | IOException e) {
                 isDead = true;
                 throw new RuntimeException(e);
             }
         }
    }
}
