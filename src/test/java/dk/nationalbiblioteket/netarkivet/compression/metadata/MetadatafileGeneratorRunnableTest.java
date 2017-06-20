package dk.nationalbiblioteket.netarkivet.compression.metadata;


import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.nationalbiblioteket.netarkivet.compression.tools.ValidateMetadataOutput;
import dk.netarkivet.common.utils.FileUtils;

import org.apache.commons.io.LineIterator;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReaderFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
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
    static String INPUT_FILE = "src/test/data/WORKING/3-metadata-1.warc.gz";
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
        org.apache.commons.io.FileUtils.deleteDirectory(new File(NMETADATA_DIR));
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
        assertTrue(output.length() > input.length(), "Expect output file to be larger than input file.");
        Runtime.getRuntime().exec("gunzip " + output).waitFor();
        File decompOutput = new File(new File(NMETADATA_DIR), "3-metadata-4.warc" );
        LineIterator li = org.apache.commons.io.FileUtils.lineIterator(decompOutput);
        String str = "alerts.log";
        boolean found = false;
        int newWarcRecords = 0;
        while (li.hasNext()) {
            String line = li.next();
            if (line.contains(str)) {
                found = true;
            }
            if (line.contains("WARC-Type")) {
                newWarcRecords++;
            }
        }
        assertTrue(found, str + " should be found in " + decompOutput.getAbsolutePath());
        input = new File(input.getParentFile(), "3-oldmetadata-1.warc.gz");
        final File inputFile = new File(new File(NMETADATA_DIR), input.getName());
        org.apache.commons.io.FileUtils.copyFile(input, inputFile);
        Runtime.getRuntime().exec("gunzip " + inputFile.getAbsolutePath()).waitFor();
        decompOutput = new File(inputFile.getParentFile(), "3-oldmetadata-1.warc");
        int oldWarcRecords = 0;
        li = org.apache.commons.io.FileUtils.lineIterator(decompOutput);
        while (li.hasNext()) {
            String line = li.next();
            if (line.contains("WARC-Type")) {
                oldWarcRecords++;
            }
        }
        assertEquals(newWarcRecords - oldWarcRecords, 2, "Expect two new records.");

    }
    @Test
    public void testValidateNewWarcFile() throws Exception {
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        metadatafileGeneratorRunnable.processFile(INPUT_FILE);
        File input = new File(INPUT_FILE);
        File output = new File(new File(NMETADATA_DIR), "3-metadata-4.warc.gz" );
        assertTrue(output.exists());
        assertTrue(output.length() > 0);
        assertTrue(WARCReaderFactory.testCompressedWARCFile(output), "Expected compressed file.");
        assertTrue(output.length() > input.length(), "Expect output file to be larger than input file.");
        File originalRenamedInput = new File(input.getParentFile(), "3-oldmetadata-1.warc.gz");  
        int recordDiff = ValidateMetadataOutput.getRecordDiff(originalRenamedInput, output);
        assertEquals(recordDiff, 2, "Expect two new records.");
        
    }
    
    @Test
    public void testProcessArcFile() throws Exception {
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        metadatafileGeneratorRunnable.processFile("src/test/data/WORKING/3-metadata-1.arc.gz");
        File input = new File("src/test/data/WORKING/3-metadata-1.arc.gz");
        File output = new File(new File(NMETADATA_DIR), "3-metadata-4.arc.gz" );
        assertTrue(output.exists());
        assertTrue(output.length() > 0);
        assertTrue(ARCReaderFactory.testCompressedARCFile(output), "Expected compressed file.");
        assertTrue(output.length() > input.length(), "Expect output file to be larger than input file.");
    }
    
    
    public void testValidateNewMetadataArcFile() throws Exception {
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        metadatafileGeneratorRunnable.processFile("src/test/data/WORKING/3-metadata-1.arc.gz");
        File input = new File("src/test/data/WORKING/3-metadata-1.arc.gz");
        File output = new File(new File(NMETADATA_DIR), "3-metadata-4.arc.gz" );
        assertTrue(output.exists());
        assertTrue(output.length() > 0);
        assertTrue(ARCReaderFactory.testCompressedARCFile(output), "Expected compressed file.");
        assertTrue(output.length() > input.length(), "Expect output file to be larger than input file.");
        int recordDiff = ValidateMetadataOutput.getRecordDiff(input, output);
        assertEquals(recordDiff, 2, "Expect two new records, but difference was: " + recordDiff);
    }
    
    @Test
    public void testcompareCrawllogWithDedupcdxfile() throws IOException, WeirdFileException, DeeplyTroublingException {
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        File originalMetadataFile = new File("src/test/data/WORKING/3-metadata-1.arc.gz");
        metadatafileGeneratorRunnable.processFile("src/test/data/WORKING/3-metadata-1.arc.gz");
        File output = new File(new File(NMETADATA_DIR), "3-metadata-4.arc.gz" );
        File dedupCdxFile = new File(new File("cdx"), originalMetadataFile.getName() + ".cdx");
        assertTrue(dedupCdxFile.exists(), "dedupcdxfile does not exist: " + dedupCdxFile.getAbsolutePath());
        boolean isValid = ValidateMetadataOutput.compareCrawllogWithDedupcdxfile(originalMetadataFile, output, dedupCdxFile);
        assertTrue(isValid, "Data should be valid");
    }

}