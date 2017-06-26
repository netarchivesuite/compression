package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.*;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileCacheSoftApacheImpl;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileLoader;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileEntryMap;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trival.IFileTriValMap;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.lang.management.ManagementFactory;
import java.util.*;
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
            return new IFileTriValMap(filename, data.getKeys(), data.getValues1(), data.getValues2());
        }
    }

    static class StaticMapLoader implements IFileMapLoader {
        private final IFileMap map;
        public StaticMapLoader(IFileMap map) {
            this.map = map;
        }
        @Override
        public IFileMap getIFileMap(String filename) throws FileNotFoundException {
            return map;
        }
    }
    static class StaticIFileLoader implements IFileLoader {
        private final ConcurrentSkipListMap<Long, IFileEntry> map;
        public StaticIFileLoader(ConcurrentSkipListMap<Long, IFileEntry> map) {
            this.map = map;
        }
        @Override
        public ConcurrentSkipListMap<Long, IFileEntry> getIFileEntryMap(String filename) throws FileNotFoundException {
            return map;
        }
    }

    /**
     * Generates random data and test lookups of IFileEntries. Secondarily outputs data on performance.
     */
    @Test
    public void monkeyTestLookups() throws FileNotFoundException {
        final int ENTRIES = 100000;
        final int LOOKUPS = 100000;
        final int RUNS = 3;
        final String WARC_NAME = "dummy";

        final int seed = new Random().nextInt();
        //final int seed = -1535339272; // Fix the seed to reproduce findings
        System.out.println("Random seed: " + seed);
        Random random = new Random(seed);

        final IFileIOHelper.IFileArrays rawData = generateSimulatedData(seed, ENTRIES);
        Map<String, IFileCache> caches = createTestCaches(WARC_NAME, rawData);
        long[] bestMS = new long[caches.size()];
        Arrays.fill(bestMS, Long.MAX_VALUE);
        System.out.println("Running " + RUNS + " iterations of " + LOOKUPS + " lookups in " + ENTRIES + " entries");
        for (int run = 0 ; run < RUNS ; run++) {
            int localSeed = random.nextInt();
            int impl = 0;
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Run #%d/%d", (run+1), RUNS));
            for (Map.Entry<String, IFileCache> entry: caches.entrySet()) {
                long ms = testLookupPerformance(
                        "localSeed=" + localSeed + ", implementation=" + entry.getKey(),
                        rawData, entry.getValue(), WARC_NAME, LOOKUPS, new Random(localSeed));
                sb.append(String.format(", %s=%d lookups/ms", entry.getKey(), LOOKUPS/ms));
                bestMS[impl] = Math.min(bestMS[impl], ms);
                impl++;
            }
            System.out.println(sb.toString());
        }

        StringBuilder sb = new StringBuilder();
        sb.append(" Fastest");
        int impl = 0;
        for (Map.Entry<String, IFileCache> entry: caches.entrySet()) {
            sb.append(", ").append(entry.getKey()).append("=").append(LOOKUPS/bestMS[impl++]).append(" lookups/ms");
        }
        System.out.println(sb.toString());
    }

    // Created from a failed {@link #MonkeyTestLookups} run (it failed because there were duplicate keys in the sample)
    @Test
    public void testSpecificTriValMap() throws FileNotFoundException {
        final int ENTRIES = 100000;
        final String WARC_NAME = "dummy";

        final int seed = -1535339272; // Fix the seed to reproduce findings
        System.out.println("Random seed: " + seed);
        final IFileIOHelper.IFileArrays rawData = generateSimulatedData(seed, ENTRIES);

        Properties properties = new Properties();
        properties.setProperty(Util.CACHE_SIZE, "300");
        Util.properties = properties;
        IFileGenericCache triValCache = new IFileGenericCache(new StaticMapLoader(
                new IFileTriValMap(WARC_NAME, rawData.getKeys(), rawData.getValues1(), rawData.getValues2())));
        for (int i = 0 ; i < ENTRIES ; i++) {
            long key = rawData.getKeys()[i];
            long newOffest = rawData.getValues1()[i];
            long timestamp = rawData.getValues2()[i];

            IFileEntry entry = triValCache.getIFileEntry(WARC_NAME, key);
            Assert.assertNotNull("For index=" + i + ", key=" + key + ", there should be an entry in " +
                    triValCache.loadFile(WARC_NAME), entry);
            Assert.assertEquals("For index=" + i + ", key=" + key + ", the newOffset should be as expected in " +
                                triValCache.loadFile(WARC_NAME),
                                Long.valueOf(newOffest), entry.getNewOffset());
            Assert.assertEquals("For index=" + i + ", key=" + key + ", the timestamp should be as expected in " +
                                triValCache.loadFile(WARC_NAME),
                                Long.valueOf(timestamp), entry.getTimestamp());
        }
    }

    // Not currently used, but nice for debug
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private String dumpSurroundings(IFileMap map, long key) {
        int origo = 0;
        for (Map.Entry<Long, IFileEntry> entry: map.entrySet()) {
            if (key == entry.getKey()) {
                break;
            }
            origo++;
        }

        StringBuilder sb = new StringBuilder(1000);
        int index = 0;
        for (Map.Entry<Long, IFileEntry> entry: map.entrySet()) {
            if (index >= origo-2 && index <= origo+2) {
                sb.append("index=" + index + ", key=" + entry.getKey() +
                          ", newOffset=" + entry.getValue().getNewOffset() +
                          ", timestamp=" + entry.getValue().getTimestamp());
            }
            index++;
        }
        return sb.toString();
    }

    private Map<String, IFileCache> createTestCaches(String warcName, IFileIOHelper.IFileArrays rawData) {
        Properties properties = new Properties();
        properties.setProperty(Util.CACHE_SIZE, "300");
        Util.properties = properties;
        Map<String, IFileCache> caches = new LinkedHashMap<>();
        IFileMap triMap = new IFileTriValMap(warcName, rawData.getKeys(), rawData.getValues1(), rawData.getValues2());
        System.out.println(triMap);
        caches.put("TriVal", new IFileGenericCache(new StaticMapLoader(
                triMap)));
        caches.put("TriLong", new IFileGenericCache(new StaticMapLoader(
                new IFileEntryMap(warcName, rawData.getKeys(), rawData.getValues1(), rawData.getValues2()))));
        ConcurrentSkipListMap<Long, IFileEntry> iFileMap = new ConcurrentSkipListMap<>();
        for (int i = 0 ; i < rawData.size() ; i++) {
            iFileMap.put(rawData.getKeys()[i], new IFileEntry(rawData.getValues1()[i], rawData.getValues2()[i]));
        }
        caches.put("Object", new IFileCacheSoftApacheImpl(new StaticIFileLoader(iFileMap)));
        return caches;
    }

    private long testLookupPerformance(String designation, IFileIOHelper.IFileArrays rawArrays, IFileCache cache,
                                       String warc, int lookups, Random random) throws FileNotFoundException {
        final long startTime = System.currentTimeMillis();
        for (int i = 0 ; i < lookups ; i++) {
            int index = random.nextInt(rawArrays.size());
            long key = rawArrays.getKeys()[index];
            IFileEntry entry = cache.getIFileEntry(warc, key);
            Assert.assertNotNull(designation + ". There should be a value for key " + key, entry);
            Assert.assertEquals(designation + ". The newOffset should be correct",
                                Long.valueOf(rawArrays.getValues1()[index]), entry.getNewOffset());
            Assert.assertEquals(designation + ". The timestamp should be correct",
                                Long.valueOf(rawArrays.getValues2()[index]), entry.getTimestamp());
        }
        return System.currentTimeMillis()-startTime;
    }

    /**
     * Attempts to generate sane sample data by limiting the offsets and timestamp to what should realistically be
     * encountered.
     * @param randomSeed used for all Random-operations and ensures reproducibility.
     * @return a thin wrapper around three arrays with oldOffsets, newOffsets and timestamps.
     */
    private static IFileIOHelper.IFileArrays generateSimulatedData(int randomSeed) {
        return generateSimulatedData(randomSeed, MAP_SIZE);
    }
    private static IFileIOHelper.IFileArrays generateSimulatedData(int randomSeed, int mapSize) {
        Random r = new Random(randomSeed);
        // Max original WARC-size 2GB
        long[] originalOffsets = r.longs(mapSize, 0,Integer.MAX_VALUE).sorted().toArray();
        // Ensure monotonically increasing
        for (int i = 1 ; i < originalOffsets.length ; i++) {
            if (originalOffsets[i-1] >= originalOffsets[i]) {
                originalOffsets[i] = originalOffsets[i-1]+1;
            }
        }
        long[] newOffsets = new long[mapSize];
        long[] timestamps = new long[mapSize];
        final long timeBase = (long) (r.nextDouble() * Integer.MAX_VALUE); // First timestamp in the WARC
        final int timeMax = (int) (r.nextDouble() * 24*60*60*1000); // Time spend harvesting the WARC (at most 1 day)

        for (int i = 0 ; i < mapSize ; i++) {
            newOffsets[i] = (long) (originalOffsets[i] * 0.7); // Simulates compression gain
            timestamps[i] = timeBase + r.nextInt(timeMax);
        }
        return new IFileIOHelper.IFileArrays().set(originalOffsets, newOffsets, timestamps, mapSize);
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