package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.*;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileTriLongLoader;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trival.IFileTriValLoader;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trival.IFileTriValMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class IFileGenericCacheTest {

    @BeforeMethod
    public void setup() throws Exception {
        MetadatafileGeneratorRunnableTest.setup();
    }

    @Test
    public void testTriLongMap() throws Exception {
        IFileGenericCache.clearInstance(); // Removes previous loader
        testBackingMap(new IFileTriLongLoader());
    }

    @Test
    public void testTriValMap() throws Exception {
        IFileGenericCache.clearInstance(); // Removes previous loader
        testBackingMap(new IFileTriValLoader());
    }

    private void testBackingMap(IFileMapLoader loader) throws Exception {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, "src/test/data/WORKING/ifiles");
        Util.properties.put(Util.IFILE_DEPTH, "2");
        Util.properties.put(Util.TEMP_DIR, "/tmp");
        Util.properties.put(Util.CACHE_SIZE, "2");
        IFileCache cache = IFileCacheFactory.getIFileCache(loader);
        assertEquals(cache.getIFileEntry("37-testfile.arc", 1500L).getNewOffset().longValue(), 1200L);
        assertEquals(cache.getIFileEntry("2-testfile.arc", 1000L).getNewOffset().longValue(), 500L);
        assertEquals(cache.getIFileEntry("371-testfile.arc", 3500L).getNewOffset().longValue(), 2700L);

    }
    
    @Test
    public void testSingleCaseTriVal() throws IOException {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, "src/test/data/WORKING/ifiles");
        Util.properties.put(Util.IFILE_DEPTH, "2");
        Util.properties.put(Util.TEMP_DIR, "/tmp");
        Util.properties.put(Util.CACHE_SIZE, "2");
        IFileIOHelper.IFileArrays arrays = IFileIOHelper.loadAsArrays("371-testfile.arc");
        for (int i = 0 ; i < arrays.size() ; i++) {
            System.out.println(arrays.getKeys()[i] + " - " + arrays.getValues1()[i] + " - " + arrays.getValues2()[i]);
        }
        System.out.println("");
        IFileTriValMap map = new IFileTriValMap(
                "371-testfile.arc", arrays.getKeys(), arrays.getValues1(), arrays.getValues2(), arrays.size());
        for (Map.Entry<Long, IFileEntry> entry: map.entrySet()) {
            System.out.println(entry.getKey() + " - " + entry.getValue());
        }
    }

}