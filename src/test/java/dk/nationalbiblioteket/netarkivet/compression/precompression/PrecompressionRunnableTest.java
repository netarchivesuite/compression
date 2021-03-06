package dk.nationalbiblioteket.netarkivet.compression.precompression;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.nationalbiblioteket.netarkivet.compression.metadata.MetadatafileGeneratorRunnableTest;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by csr on 12/8/16.
 */
public class PrecompressionRunnableTest {

    //File outputDir;
    //String inputFile = "src/test/data/456-foobar.warc";
    String inputFile = "src/test/data/WORKING/3-1-20161205101105604-00000-14970.arc";

    Logger logger = LoggerFactory.getLogger(PrecompressionRunnableTest.class);

    @org.testng.annotations.BeforeClass
    public void setUp() throws Exception {
        cleanup();
        MetadatafileGeneratorRunnableTest.setup();
    }

    @org.testng.annotations.AfterClass
    public void tearDown() throws Exception {
        cleanup();
    }

    private void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File("ifiles"));
        FileUtils.deleteDirectory(new File("cdxes"));
        FileUtils.deleteDirectory(new File("output"));
    }

    @DataProvider(name = "fileNameProvider")
    public static Iterator<Object[]> getFiles() {
        File[] files = new File("src/test/data/WORKING/precompress").listFiles((dir, name) -> {
            //return name.endsWith("arc");
            return name.startsWith("3-3");
        }) ;

/*         File[] files = new File[]{
                 new File("src/test/data/WORKING/4868-metadata-1.arc"),
                 new File("src/test/data/WORKING/4868-metadata-2.arc"),
                 new File("src/test/data/WORKING/4868-metadata-3.arc"),
                 new File("src/test/data/WORKING/1545-metadata-2.arc"),
                 new File("src/test/data/WORKING/733-44-20101217211223-00007-sb-test-har-001.statsbiblioteket.dk.arc"),
                 new File("src/test/data/WORKING/3-1-20161205101105604-00000-14970.arc"),
                 new File("src/test/data/WORKING/1185-77-20110304134905-00003-kb-test-har-002.kb.dk.arc"),
                 new File("src/test/data/WORKING/2-2-20161205125206020-00000-kb-test-har-004.kb.dk.warc")
         };*/
        List<Object[]> list = new ArrayList<>();
        for (File file: files) {
            list.add(new File[]{file});
        }
        return list.iterator();
    }

    static {
        SLF4JBridgeHandler.install();
    }

    @Test(dataProvider = "fileNameProvider")
    @Parameters("inputFile")
    public void testPrecompress(File inputFile) throws WeirdFileException, DeeplyTroublingException, IOException {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, "ifiles" );
        Util.properties.put(Util.CDX_ROOT_DIR, "cdxes");
        Util.properties.put(Util.IFILE_DEPTH, "4");
        Util.properties.put(Util.CDX_DEPTH, "0");
        Util.properties.put(Util.MD5_FILEPATH,   "output/checksum_CS.md5");
        Util.properties.put(Util.TEMP_DIR, "output");
        Util.properties.put(Util.THREADS, "10");
        Util.properties.put(Util.LOG, "output/log");
        File ifileSubdir = Util.getIFileSubdir(inputFile.getName(), false);
        File ifile = new File(ifileSubdir, inputFile.getName() + ".ifile.cdx");
        assertFalse(ifile.exists());
        PrecompressionRunnable consumer = new PrecompressionRunnable(null, 0);
        consumer.precompress(inputFile.getAbsolutePath());
        assertTrue(ifile.exists(), ifile.getAbsolutePath() + " should exist.");
        boolean isMetadata = inputFile.getName().contains("metadata"); 
        assertTrue( (isMetadata && ifile.length()==0) || FileUtils.readLines(ifile).size() > 10, "Failed because either inputfile is metadata and ifile is empty, or ifile-length <= 10.Ifile-length=" +  ifile.length());
    }

}