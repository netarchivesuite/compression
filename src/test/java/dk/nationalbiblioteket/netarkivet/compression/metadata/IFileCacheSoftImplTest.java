package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileCache;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileCacheSoftApacheImpl;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileLoader;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileCacheSoftLongArrays;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileEntryMap;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileTriLongLoader;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by csr on 5/22/17.
 */
public class IFileCacheSoftImplTest {

    private static final int MAP_SIZE = 10000;
    private static final int INSERTS = 50000;
    private static final int LOG_INTERVAL = 100;

    static class TestIFileLoader implements IFileLoader {

        @Override
        public ConcurrentSkipListMap<Long, IFileEntry> getIFileEntryMap(String filename) throws FileNotFoundException {
            ConcurrentSkipListMap<Long, IFileEntry> iFileMap = new ConcurrentSkipListMap<>();
            Random random = new Random();
            for (int i=0; i<MAP_SIZE; i++) {
                long l = random.nextLong();
                iFileMap.put(l, new IFileEntry(l, l));
            }
            return iFileMap;
        }
    }

    static class TestIFileTriLongLoader implements IFileTriLongLoader {

        @Override
        public IFileEntryMap getIFileEntryMap(String filename) throws FileNotFoundException {
            Random r = new Random();
            long []  originalOffsets = r.longs(MAP_SIZE).sorted().toArray();
            return new IFileEntryMap(filename, originalOffsets, originalOffsets, originalOffsets);
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
        //IFileCache cache = new IFileCacheSoftLongArrays(new )
        for (long i = 1; i < INSERTS; i++) {
            cache.getOrderedListing(UUID.randomUUID().toString());
            if (i%LOG_INTERVAL == 0) {
                System.out.println("Added " + i + ", " + cache.toString());
                System.out.println("Memory (max/total/free) " + runtime.maxMemory() + "/" + runtime.totalMemory() + "/" + runtime.freeMemory());
            }
        }
    }

    //@Test
    public void testGetOrderedListingTriLong() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Util.CACHE_SIZE, "0");
        Util.properties = properties;
        Runtime runtime = Runtime.getRuntime();
        int mb = 1024*1024;
        IFileCache cache = IFileCacheSoftLongArrays.getIFileCacheSoftApacheImpl(new TestIFileTriLongLoader());
        for (long i = 1; i < INSERTS; i++) {
            cache.getOrderedListing(UUID.randomUUID().toString());
            if (i%LOG_INTERVAL == 0) {
                System.out.println("Added " + i + ", " + cache.toString());
                System.out.println("Memory (max/total/free) " + runtime.maxMemory() + "/" + runtime.totalMemory() + "/" + runtime.freeMemory());
            }
        }
    }

}