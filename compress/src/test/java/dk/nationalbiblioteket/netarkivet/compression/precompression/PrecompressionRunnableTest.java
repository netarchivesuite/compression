package dk.nationalbiblioteket.netarkivet.compression.precompression;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Properties;

/**
 * Created by csr on 12/8/16.
 */
public class PrecompressionRunnableTest {

    String OUTPUT_ROOT_DIR = "output";
    File outputDir;
    //String inputFile = "src/test/data/456-foobar.warc";
    String inputFile = "src/test/data/3-1-20161205101105604-00000-14970~sb-test-har-001.statsbiblioteket.dk~8171.arc";


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
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, OUTPUT_ROOT_DIR);
        Util.properties.put("DEPTH", "4");
        Util.properties.put("MD5_FILEPATH", OUTPUT_ROOT_DIR + "/checksum_CS.md5");
        Util.properties.put(Util.TEMP_DIR, "/tmp");
        PrecompressionRunnable consumer = new PrecompressionRunnable(null, 0);
        consumer.precompress(inputFile);
    }

}