package dk.nationalbiblioteket.netarkivet.compression.precompression;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by csr on 12/8/16.
 */
public class ConsumerTest {

    String OUTPUT_ROOT_DIR = "output";
    File outputDir;
    String inputFile = "src/test/data/456-foobar.warc";

    @org.testng.annotations.BeforeMethod
    public void setUp() throws Exception {
        outputDir = new File(OUTPUT_ROOT_DIR);
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
        outputDir.mkdirs();
    }

    @org.testng.annotations.AfterMethod
    public void tearDown() throws Exception {
        //FileUtils.deleteDirectory(outputDir);
    }

    @org.testng.annotations.Test
    public void testPrecompress() throws Exception {
        PreCompressor.properties = new Properties();
        PreCompressor.properties.put("OUTPUT_ROOT_DIR", OUTPUT_ROOT_DIR);
        PreCompressor.properties.put("DEPTH", "4");
        PreCompressor.properties.put("MD5_FILEPATH", OUTPUT_ROOT_DIR + "/checksum_CS.md5");
        Consumer consumer = new Consumer(null, 0);
        consumer.precompress(inputFile);
    }

}