package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.netarkivet.common.utils.FileUtils;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReaderFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Properties;

import static org.testng.Assert.*;

/**
 * Created by csr on 1/5/17.
 */
public class MetadatafileGeneratorRunnableTest {

    static String originals ="src/test/data/ORIGINALS";
    static String working ="src/test/data/WORKING";
    static String IFILE_DIR = "src/test/data/WORKING/ifiles";
    static String Depth = "4";
    static String INPUT_FILE = "src/test/data/WORKING/3-metadata-1.warc";
    static String NMETADATA_DIR = "output";

    @BeforeMethod
    public static void setup() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(new File(working));
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, IFILE_DIR);
        Util.properties.put(Util.IFILE_DEPTH, Depth);
        Util.properties.put(Util.NMETADATA_DIR, NMETADATA_DIR);
        Util.properties.put(Util.CACHE_SIZE, "1000");
        Util.properties.put(Util.METADATA_GENERATION, "4");
        Util.properties.put(Util.CDX_ROOT_DIR, "cdx");
        Util.properties.put(Util.CDX_DEPTH, "0");
        Util.properties.put(Util.UPDATED_FILENAME_MD5_FILEPATH, NMETADATA_DIR + "/newchecksums");
        FileUtils.copyDirectory(new File(originals), new File(working));
    }

    @Test
    public void testProcessWarcFile() throws Exception {
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        metadatafileGeneratorRunnable.processFile(INPUT_FILE);
        File input = new File(INPUT_FILE);
        File output = new File(new File(NMETADATA_DIR), "3-metadata-4.warc.gz" );
        assertTrue(output.exists());
        assertTrue(output.length() > 0);
        assertTrue(WARCReaderFactory.testCompressedWARCFile(output), "Expected compressed file.");
    }

    @Test
    public void testProcessArcFile() throws Exception {
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        metadatafileGeneratorRunnable.processFile("src/test/data/WORKING/3-metadata-1.arc");
        File input = new File(INPUT_FILE);
        File output = new File(new File(NMETADATA_DIR), "3-metadata-4.arc.gz" );
        assertTrue(output.exists());
        assertTrue(output.length() > 0);
        assertTrue(ARCReaderFactory.testCompressedARCFile(output), "Expected compressed file.");
    }



}