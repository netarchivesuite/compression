package dk.nationalbiblioteket.netarkivet.compression.precompression;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.apache.commons.io.FileUtils;
import org.jwat.tools.tasks.cdx.CDXFile;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertTrue;

/**
 * Created by csr on 12/8/16.
 */
public class PrecompressionRunnableTest {

    //File outputDir;
    //String inputFile = "src/test/data/456-foobar.warc";
    String inputFile = "src/test/data/3-1-20161205101105604-00000-14970.arc";


    @org.testng.annotations.BeforeMethod
    public void setUp() throws Exception {
       cleanup();
    }

    @org.testng.annotations.AfterMethod
    public void tearDown() throws Exception {
        //cleanup();
    }

    private void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File("ifiles"));
        FileUtils.deleteDirectory(new File("cdxes"));
        FileUtils.deleteDirectory(new File("output"));
    }

    @org.testng.annotations.Test
    public void testPrecompress() throws Exception {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, "ifiles" );
        Util.properties.put(Util.CDX_ROOT_DIR, "cdxes");
        Util.properties.put(Util.IFILE_DEPTH, "4");
        Util.properties.put(Util.CDX_DEPTH, "0");
        Util.properties.put(Util.MD5_FILEPATH,   "output/checksum_CS.md5");
        Util.properties.put(Util.TEMP_DIR, "output");
        Util.properties.put(Util.THREADS, "10");
        Util.properties.put(Util.LOG, "output/log");
        PrecompressionRunnable consumer = new PrecompressionRunnable(null, 0);
        consumer.precompress(inputFile);
        File f1 = new File("ifiles/0/0/0/3/3-1-20161205101105604-00000-14970.arc.ifile.cdx");
        assertTrue(f1.exists(), f1.getAbsolutePath());
    }

   @org.testng.annotations.Test
    public void testPrecompressArc() throws Exception {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, "ifiles" );
        Util.properties.put(Util.CDX_ROOT_DIR, "cdxes");
        Util.properties.put(Util.IFILE_DEPTH, "4");
        Util.properties.put(Util.CDX_DEPTH, "0");
        Util.properties.put(Util.MD5_FILEPATH,   "output/checksum_CS.md5");
        Util.properties.put(Util.TEMP_DIR, "output");
        Util.properties.put(Util.THREADS, "10");
        Util.properties.put(Util.LOG, "output/log");
        PrecompressionRunnable consumer = new PrecompressionRunnable(null, 0);
        consumer.precompress("src/test/data/1185-77-20110304134905-00003-kb-test-har-002.kb.dk.arc");
    }


    @org.testng.annotations.Test
       public void testPrecompressWarc() throws Exception {
           Util.properties = new Properties();
           Util.properties.put(Util.IFILE_ROOT_DIR, "ifiles" );
           Util.properties.put(Util.CDX_ROOT_DIR, "cdxes");
           Util.properties.put(Util.IFILE_DEPTH, "4");
           Util.properties.put(Util.CDX_DEPTH, "0");
           Util.properties.put(Util.MD5_FILEPATH,   "output/checksum_CS.md5");
           Util.properties.put(Util.TEMP_DIR, "output");
           Util.properties.put(Util.THREADS, "10");
           Util.properties.put(Util.LOG, "output/log");
           PrecompressionRunnable consumer = new PrecompressionRunnable(null, 0);
           consumer.precompress("/home/csr/temp/2-2-20161205125206020-00000-kb-test-har-004.kb.dk.warc");
       }


}