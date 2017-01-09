package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.testng.annotations.Test;
import org.testng.reporters.jq.INavigatorPanel;

import java.io.File;
import java.util.Properties;

import static org.testng.Assert.*;

/**
 * Created by csr on 1/5/17.
 */
public class MetadatafileGeneratorRunnableTest {

    String IFILE_DIR = "src/test/data/ifiles";
    String Depth = "4";
    String INPUT_FILE = "src/test/data/3-metadata-1.warc";
    String NMETADATA_DIR = "output";

    @Test
    public void testProcessWarcFile() throws Exception {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, IFILE_DIR);
        Util.properties.put(Util.DEPTH, Depth);
        Util.properties.put(Util.NMETADATA_DIR, NMETADATA_DIR);
        Util.properties.put(Util.CACHE_SIZE, "1000");
        Util.properties.put(Util.METADATA_GENERATION, "4");
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        metadatafileGeneratorRunnable.processFile(INPUT_FILE);
        File input = new File(INPUT_FILE);
        File output = new File(new File(NMETADATA_DIR), "3-metadata-4.warc.gz" );
        assertTrue(output.exists());
        assertTrue(output.length() > 0);
    }

    @Test
    public void testProcessArcFile() throws Exception {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, IFILE_DIR);
        Util.properties.put(Util.DEPTH, Depth);
        Util.properties.put(Util.NMETADATA_DIR, NMETADATA_DIR);
        Util.properties.put(Util.CACHE_SIZE, "1000");
        Util.properties.put(Util.METADATA_GENERATION, "4");
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        metadatafileGeneratorRunnable.processFile("src/test/data/3-metadata-1.arc");
        File input = new File(INPUT_FILE);
        File output = new File(new File(NMETADATA_DIR), "3-metadata-4.arc.gz" );
        assertTrue(output.exists());
        assertTrue(output.length() > 0);
    }

}