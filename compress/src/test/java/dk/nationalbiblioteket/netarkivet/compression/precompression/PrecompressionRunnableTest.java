package dk.nationalbiblioteket.netarkivet.compression.precompression;

import dk.nationalbiblioteket.netarkivet.compression.FatalException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import org.apache.commons.collections.list.AbstractLinkedList;
import org.apache.commons.io.FileUtils;
import org.jwat.tools.tasks.cdx.CDXFile;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

    @DataProvider(name = "fileNameProvider")
    public static Iterator<Object[]> getFiles() {
         File[] files = new File[]{
                 new File("src/test/data/3-1-20161205101105604-00000-14970.arc"),
                 new File("src/test/data/1185-77-20110304134905-00003-kb-test-har-002.kb.dk.arc"),
                 new File("src/test/data/2-2-20161205125206020-00000-kb-test-har-004.kb.dk.warc")
         };
        List<Object[]> list = new ArrayList<>();
        for (File file: files) {
            list.add(new File[]{file});
        }
        return list.iterator();
    }

    @Test(dataProvider = "fileNameProvider")
    @Parameters("inputFile")
    public void testPrecompress(File inputFile) throws WeirdFileException, FatalException, IOException {
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
        assertTrue(FileUtils.readLines(ifile).size() > 10);
    }

}