package dk.nationalbiblioteket.netarkivet.compression;

import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.*;

/**
 * Created by csr on 1/9/17.
 */
public class UtilTest {
    @Test
    public void testGetNewMetadataFilename() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Util.METADATA_GENERATION, "4");
        String file = "/hello/world/456-metadata-2.warc";
        Util.properties = properties;
        String output = Util.getNewMetadataFilename(file);
        assertEquals(output, "456-metadata-4.warc.gz");
        file = "/hello/world/456-metadata-2.arc";
        output = Util.getNewMetadataFilename(file);
        assertEquals(output, "456-metadata-4.arc.gz");
    }

}