package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.precompression.PreCompressor;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.*;

/**
 * Created by csr on 12/22/16.
 */
public class IFileCacheImplTest {
    @Test
    public void testGetIFileEntry() throws Exception {
        PreCompressor.properties = new Properties();
        PreCompressor.properties.put("OUTPUT_ROOT_DIR", "src/test/data/ifiles");
        PreCompressor.properties.put("DEPTH", "2");
        PreCompressor.properties.put(PreCompressor.TEMP_DIR, "/tmp");
        IFileCacheImpl cache = new IFileCacheImpl(2);
        assertEquals(cache.getIFileEntry("37-testfile.arc", 1500L).getNewOffset().longValue(), 1200L);
        assertEquals(cache.getIFileEntry("2-testfile.arc", 1000L).getNewOffset().longValue(), 500L);
        assertEquals(cache.getIFileEntry("371-testfile.arc", 3500L).getNewOffset().longValue(), 2700L);

    }

}