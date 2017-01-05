package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.*;

/**
 * Created by csr on 12/22/16.
 */
public class IFileCacheImplTest {
    @Test
    public void testGetIFileEntry() throws Exception {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, "src/test/data/ifiles");
        Util.properties.put(Util.DEPTH, "2");
        Util.properties.put(Util.TEMP_DIR, "/tmp");
        Util.properties.put(Util.CACHE_SIZE, "2");
        IFileCacheImpl cache = IFileCacheImpl.getIFileCacheImpl();
        assertEquals(cache.getIFileEntry("37-testfile.arc", 1500L).getNewOffset().longValue(), 1200L);
        assertEquals(cache.getIFileEntry("2-testfile.arc", 1000L).getNewOffset().longValue(), 500L);
        assertEquals(cache.getIFileEntry("371-testfile.arc", 3500L).getNewOffset().longValue(), 2700L);

    }

}