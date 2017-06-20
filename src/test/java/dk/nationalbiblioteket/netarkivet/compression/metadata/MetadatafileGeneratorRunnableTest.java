package dk.nationalbiblioteket.netarkivet.compression.metadata;

import com.hp.gagawa.java.elements.Li;
import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.netarkivet.common.utils.FileUtils;
import org.apache.commons.io.LineIterator;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReaderFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.testng.Assert.*;

/**
 * Created by csr on 1/5/17.
 */
public class MetadatafileGeneratorRunnableTest {

    static String originals ="src/test/data/ORIGINALS/metadata";
    static String working ="src/test/data/WORKING/metadata";
    static String IFILE_DIR = "src/test/data/WORKING/metadata/ifiles";
    static String Depth = "0";
    static String INPUT_FILE = "src/test/data/WORKING/3-metadata-1.warc.gz";
    static String NMETADATA_DIR = "output";

    @BeforeClass
    public static void setup() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(new File(working));
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, IFILE_DIR);
        Util.properties.put(Util.IFILE_DEPTH, Depth);
        Util.properties.put(Util.NMETADATA_DIR, NMETADATA_DIR);
        Util.properties.put(Util.CACHE_SIZE, "0");
        Util.properties.put(Util.METADATA_GENERATION, "4");
        Util.properties.put(Util.CDX_ROOT_DIR, "cdx");
        Util.properties.put(Util.CDX_DEPTH, "0");
        Util.properties.put(Util.UPDATED_FILENAME_MD5_FILEPATH, NMETADATA_DIR + "/newchecksums");
        Util.properties.put(Util.USE_SOFT_CACHE, "true");
        FileUtils.copyDirectory(new File(originals), new File(working));
        org.apache.commons.io.FileUtils.deleteDirectory(new File(NMETADATA_DIR));

    }

    @DataProvider(name = "fileNameProvider")
    public static Iterator<Object[]> getFiles() {
        File[] files = new File(working).listFiles((dir, name) -> {
            return name.endsWith(".gz");
        }) ;
        List<Object[]> list = new ArrayList<>();
        for (File file: files) {
            list.add(new File[]{file});
        }
        return list.iterator();
    }


    @Test(dataProvider = "fileNameProvider")
    @Parameters("inputFile")
    public void testProcess(File inputFile) throws DeeplyTroublingException, WeirdFileException, IOException, InterruptedException {
        MetadatafileGeneratorRunnable metadatafileGeneratorRunnable = new MetadatafileGeneratorRunnable(null, 0);
        metadatafileGeneratorRunnable.processFile(inputFile.getAbsolutePath());
        String path = inputFile.getAbsolutePath();
        path = path.replace("1.arc.gz", "4.arc.gz");
        path = path.replace("2.arc.gz", "4.arc.gz");
        path = path.replace("1.warc.gz", "4.warc.gz");
        File output = new File(new File(NMETADATA_DIR), new File(path).getName());
        assertTrue(output.exists(), output.getAbsolutePath() + " should exist");
        assertTrue(output.length() > 0);
        assertTrue(WARCReaderFactory.testCompressedWARCFile(output), "Expected compressed file.");
        assertTrue(output.length() > inputFile.length(), "Expect output file to be larger than input file.");
        Runtime.getRuntime().exec("gunzip " + output).waitFor();
        File decompOutput = new File(output.getAbsolutePath().replace(".gz", ""));
        inputFile = new File(inputFile.getParentFile(), inputFile.getName().replace("metadata", "oldmetadata"));
        File compInput = new File(new File(NMETADATA_DIR), inputFile.getName());
        org.apache.commons.io.FileUtils.copyFile(inputFile, compInput);
        Runtime.getRuntime().exec("gunzip " + compInput.getAbsolutePath()).waitFor();
        File decompInput = new File(compInput.getAbsolutePath().replace(".gz", ""));
        if (inputFile.getName().endsWith("warc.gz")) {
            checkWarc(decompInput, decompOutput);
        } else {
            checkArc(decompInput, decompOutput);
        }
        System.out.println(Util.getMemoryStats());
    }

    private void checkArc(File decompInput, File decompOutput) throws IOException {
        LineIterator outputLI = org.apache.commons.io.FileUtils.lineIterator(decompOutput);
        LineIterator inputLI = org.apache.commons.io.FileUtils.lineIterator(decompInput);
        long inputCount = StreamSupport.stream(Spliterators.spliteratorUnknownSize(inputLI, Spliterator.ORDERED), false).filter(line -> line.contains("metadata://")).count();
        long outputCount = StreamSupport.stream(Spliterators.spliteratorUnknownSize(outputLI, Spliterator.ORDERED), false).filter(line -> line.contains("metadata://")).count();
        assertEquals(outputCount-inputCount, 2L);
    }

    private void checkWarc(File decompInput, File decompOutput) throws IOException {
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
        // assertTrue(found, str + " should be found in " + decompOutput.getAbsolutePath());
        int oldWarcRecords = 0;
        li = org.apache.commons.io.FileUtils.lineIterator(decompInput);
        while (li.hasNext()) {
            String line = li.next();
            if (line.contains("WARC-Type")) {
                oldWarcRecords++;
            }
        }
        assertEquals(newWarcRecords - oldWarcRecords, 2, "Expect two new records.");
    }






}