package dk.nationalbiblioteket.netarkivet.compression.precompression;

import dk.nationalbiblioteket.netarkivet.compression.metadata.MetadatafileGeneratorRunnableTest;
import org.jwat.tools.tasks.cdx.CDXFile;
import org.jwat.tools.tasks.cdx.CDXFormatter;
import org.jwat.tools.tasks.cdx.CDXOptions;
import org.testng.annotations.BeforeMethod;

import java.io.File;

import static org.testng.Assert.assertTrue;

/**
 * Created by csr on 2/27/17.
 */
public class CdxTest extends CDXFile {

    @BeforeMethod
    public void setup() throws Exception {
        MetadatafileGeneratorRunnableTest.setup();
    }

    @org.testng.annotations.Test
    public void testCdxGen() {
        File input = new File("src/test/data/WORKING/precompress/1185-77-20110304134905-00003-kb-test-har-002.kb.dk.arc");
        CDXFile cdxFile = new CDXFile();
    	final int recordHeaderMaxSize = 1024 * 1024;
        final int payloadHeaderMaxSize = 1024 * 1024;
        CDXOptions cdxOptions = new CDXOptions();
        cdxOptions.recordHeaderMaxSize = recordHeaderMaxSize;
        cdxOptions.payloadHeaderMaxSize = payloadHeaderMaxSize;
        cdxFile.processFile(input, cdxOptions);

        assertTrue(true);
    }
}
