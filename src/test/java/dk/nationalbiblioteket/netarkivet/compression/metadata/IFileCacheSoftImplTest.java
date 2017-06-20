package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.testng.Assert.*;

/**
 * Created by csr on 5/22/17.
 */
public class IFileCacheSoftImplTest {

    private static final int MAP_SIZE = 10000;
    private static final int INSERTS = 1000000;
    private static final int LOG_INTERVAL = 100;

    static class TestIFileLoader implements IFileLoader {

        @Override
        public ConcurrentSkipListMap<Long, IFileEntry> getIFileEntryMap(String filename) throws FileNotFoundException {
            ConcurrentSkipListMap<Long, IFileEntry> iFileMap = new ConcurrentSkipListMap<>();
            Random random = new Random();
            for (int i=0; i<MAP_SIZE; i++) {
                iFileMap.put(random.nextLong(), new IFileEntry(random.nextLong(), random.nextLong()));
            }
            return iFileMap;
        }
    }

    //@Test
    public void testGetOrderedListing() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Util.CACHE_SIZE, "300");
        Util.properties = properties;
        Runtime runtime = Runtime.getRuntime();
        int mb = 1024*1024;
        IFileCacheSoftApacheImpl cache = IFileCacheSoftApacheImpl.getIFileCacheSoftApacheImpl(new TestIFileLoader());
        for (long i = 1; i < INSERTS; i++) {
            cache.getOrderedListing(UUID.randomUUID().toString());
            if (i%LOG_INTERVAL == 0) {
                System.out.println("Added " + i + ", " + cache.toString());
                //System.out.println("Memory (max/total/free) " + runtime.maxMemory() + "/" + runtime.totalMemory() + "/" + runtime.freeMemory());
            }
        }
    }

}