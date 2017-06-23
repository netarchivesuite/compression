package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.*;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileCacheSoftApacheImpl;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileLoader;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileEntryMap;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trival.IFileTriValMap;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Random;
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
            IFileIOHelper.IFileArrays data = generateSimulatedData(filename.hashCode());
            ConcurrentSkipListMap<Long, IFileEntry> iFileMap = new ConcurrentSkipListMap<>();
            for (int i = 0 ; i < data.size() ; i++) {
                iFileMap.put(data.getKeys()[i], new IFileEntry(data.getValues1()[i], data.getValues2()[i]));
            }
            return iFileMap;
        }
    }

    static class TestIFileTriLongLoader implements IFileMapLoader {
        @Override
        public IFileMap getIFileMap(String filename) throws FileNotFoundException {
            IFileIOHelper.IFileArrays data = generateSimulatedData(filename.hashCode());
            return new IFileEntryMap(filename, data.getKeys(), data.getValues1(), data.getValues2());
        }
    }

    static class TestIFileTriValLoader implements IFileMapLoader {
        @Override
        public IFileMap getIFileMap(String filename) throws FileNotFoundException {
            IFileIOHelper.IFileArrays data = generateSimulatedData(filename.hashCode());
            IFileMap map = new IFileTriValMap(filename, data.getKeys(), data.getValues1(), data.getValues2());
            //System.out.println(map);
            return map;
        }
    }

    /**
     * Attempts to generate sane sample data by limiting the offsets and timestamp to what should realistically be
     * encountered.
     * @param randomSeed used for all Random-operations and ensures reproducibility.
     * @return a thin wrapper around three arrays with oldOffsets, newOffsets and timestamps.
     */
    private static IFileIOHelper.IFileArrays generateSimulatedData(int randomSeed) {
        Random r = new Random(randomSeed);
        // Max original WARC-size 2GB
        long[] originalOffsets = r.longs(MAP_SIZE, 0,Integer.MAX_VALUE).sorted().toArray();
        long[] newOffsets = new long[MAP_SIZE];
        long[] timestamps = new long[MAP_SIZE];
        final long timeBase = (long) (r.nextDouble() * Integer.MAX_VALUE); // First timestamp in the WARC
        final int timeMax = (int) (r.nextDouble() * 24*60*60*1000); // Time spend harvesting the WARC (at most 1 day)

        for (int i = 0 ; i < MAP_SIZE ; i++) {
            newOffsets[i] = (long) (originalOffsets[i] * 0.7); // Simulates compression gain
            timestamps[i] = timeBase + r.nextInt(timeMax);
        }
        return new IFileIOHelper.IFileArrays().set(originalOffsets, newOffsets, timestamps, MAP_SIZE);
    }

    //@Test
    public void testGetOrderedListing() throws Exception {
        System.out.println("Performance test for the Object implementation");
        Properties properties = new Properties();
        properties.setProperty(Util.CACHE_SIZE, "300");
        Util.properties = properties;
        performSpeedTest(IFileCacheSoftApacheImpl.getIFileCacheSoftApacheImpl(new TestIFileLoader()));
    }

    //@Test
    public void testGetOrderedListingTriLong() throws Exception {
        System.out.println("Performance test for the TriLong implementation");
        testGetOrderedListingGeneric(new TestIFileTriLongLoader());
    }
    //@Test
    public void testGetOrderedListingTriVal() throws Exception {
        System.out.println("Performance test for the TriVal implementation");
        testGetOrderedListingGeneric(new TestIFileTriValLoader());
    }

    // Runs performance tests for Object, TriLong and TriVal implementations, collecting process time and
    // maximum cache size for comparison when the test has finished.
    //@Test
    public void compareImplementations() throws Exception {
        final int IMPLS = 3;
        final String[] implName = new String[IMPLS];
        final long[] processMS = new long[IMPLS];
        final long[] gcMS = new long[IMPLS];
        final int[] maxCacheSizes = new int[IMPLS];

        Properties properties = new Properties();
        properties.setProperty(Util.CACHE_SIZE, "300");
        Util.properties = properties;

        int index = 0;
        {
            System.gc();
            long previousGCTime = getGCTime();
            implName[index] = "TriVal";
            long startTime = System.currentTimeMillis();
            maxCacheSizes[index] = testGetOrderedListingGeneric(new TestIFileTriValLoader());
            processMS[index] = System.currentTimeMillis()-startTime;
            long gcTime = getGCTime();
            gcMS[index] = gcTime-previousGCTime;
            IFileGenericCache.clearInstance(); // Clean up for next test
            index++;
        }
        {
            System.gc();
            long previousGCTime = getGCTime();
            implName[index] = "TriLong";
            long startTime = System.currentTimeMillis();
            maxCacheSizes[index] = testGetOrderedListingGeneric(new TestIFileTriLongLoader());
            processMS[index] = System.currentTimeMillis()-startTime;
            long gcTime = getGCTime();
            gcMS[index] = gcTime-previousGCTime;
            IFileGenericCache.clearInstance(); // Clean up for next test
            index++;
        }
        {
            System.gc();
            long previousGCTime = getGCTime();
            implName[index] = "Object";
            long startTime = System.currentTimeMillis();
            maxCacheSizes[index] = performSpeedTest(
                    IFileCacheSoftApacheImpl.getIFileCacheSoftApacheImpl(new TestIFileLoader()));
            processMS[index] = System.currentTimeMillis()-startTime;
            long gcTime = getGCTime();
            gcMS[index] = gcTime-previousGCTime;
            index++;
        }

        System.out.println("\nSpeed & space for maps with " + index + " entries");
        for (int i = 0 ; i < index ; i++) {
            System.out.println(String.format("%s-impl: %d seconds, %d max cache size, %s seconds GC time",
                                             implName[i], processMS[i]/1000, maxCacheSizes[i], gcMS[i]/1000));
        }
        /*
-Xmx1024
TriVal-impl: 61 seconds, 8252 max cache size, 7 seconds GC time
TriLong-impl: 55 seconds, 3472 max cache size, 9 seconds GC time
Object-impl: 239 seconds, 631 max cache size, 133 seconds GC time

-Xmx4096
TriVal-impl: 58 seconds, 33290 max cache size, 6 seconds GC time
TriLong-impl: 53 seconds, 13898 max cache size, 8 seconds GC time
Object-impl: 371 seconds, 2529 max cache size, 263 seconds GC time

         */
    }

    private long getGCTime() {
        long gcTime = 0;
        for(java.lang.management.GarbageCollectorMXBean gc :
            ManagementFactory.getGarbageCollectorMXBeans()) {
            long time = gc.getCollectionTime();
            if (time >= 0) {
                gcTime += time;
            }
        }
        return gcTime;
    }

    private int testGetOrderedListingGeneric(IFileMapLoader mapLoader) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Util.CACHE_SIZE, "0");
        Util.properties = properties;
        return performSpeedTest(IFileCacheFactory.getIFileCache(mapLoader));
    }

    // Returns maximum cache size
    private int performSpeedTest(IFileCache cache) throws FileNotFoundException {
        final long startTime = System.currentTimeMillis();
        final Random random = new Random(87); // Fixed sequence of randoms for reproducibility
        Runtime runtime = Runtime.getRuntime();
        int maxCacheSize = 0;
        long lastGCTime = getGCTime();
        final long baseGCTime = lastGCTime;
        for (long i = 1; i < INSERTS; i++) {
            cache.getOrderedListing(Integer.toString(random.nextInt()));
            if (i%LOG_INTERVAL == 0) {
                long gcTimeTotal = getGCTime();
                long gcTime = gcTimeTotal-lastGCTime;
                lastGCTime = gcTimeTotal;
                System.out.println(String.format(
                        "Added %d maps, Cache size %d, Memory(max/total/free) %d/%d/%d, GC %d ms",
                        i, cache.getCurrentCachesize(),
                        mb(runtime.maxMemory()), mb(runtime.totalMemory()), mb(runtime.freeMemory()), gcTime));
            }
            maxCacheSize = Math.max(maxCacheSize, cache.getCurrentCachesize());
        }
        System.out.println("Maximum cache size during test: " + maxCacheSize);
        System.out.println("Test time: " + (System.currentTimeMillis()-startTime)/1000 + " seconds");
        System.out.println("Total GC time during test: " + (lastGCTime-getGCTime())/1000 + " seconds");
        return maxCacheSize;
    }

    public long mb(long bytes) {
        return bytes/1048576;
    }
}