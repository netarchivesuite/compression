package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileCache;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

/**
 *
 */
public class IFileCacheImpl implements IFileCache {

    private static IFileCacheImpl instance;
    private IFileLoader iFileLoader;
    private final ArrayBlockingQueue<String> elementQueue;
    private ConcurrentHashMap<String, Future<ConcurrentSkipListMap<Long, IFileEntry>>> cache = new ConcurrentHashMap<>();
    // TODO: 40 threads are pretty arbitrary - maybe make this a property?
    private final ExecutorService executor = Executors.newFixedThreadPool(40, new ThreadFactory() {
        private int loaderID = 0;
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "iFileLoader_" + loaderID++);
            t.setDaemon(true);
            return t;
        }
    });

    private IFileCacheImpl(IFileLoader iFileLoader) {
        int cacheSize = Integer.parseInt(Util.getProperties().getProperty(Util.CACHE_SIZE));
        elementQueue = new ArrayBlockingQueue<>(cacheSize);
        this.iFileLoader = iFileLoader;
    }

    public static synchronized IFileCacheImpl getIFileCacheImpl(IFileLoader iFileLoader) {
        if (instance == null) {
            instance = new IFileCacheImpl(iFileLoader);
        }
        return instance;
    }

    @Override
    public IFileEntry getIFileEntry(String oldFilename, Long oldOffset) throws FileNotFoundException {
        return getOffsetMap(oldFilename).get(oldOffset);
    }

    @Override
    public Iterator<Map.Entry<Long, IFileEntry>> getOrderedListing(String oldFilename) throws FileNotFoundException {
        return getOffsetMap(oldFilename).entrySet().iterator();
    }

    @Override
    public int getCurrentCachesize() {
        return cache.size();
    }

    private Map<Long, IFileEntry> getOffsetMap(String oldFilename) throws FileNotFoundException {
        Future<ConcurrentSkipListMap<Long, IFileEntry>> offsetMap;

        synchronized (elementQueue) {
            offsetMap = cache.get(oldFilename);

            if (offsetMap == null) {
                // We need to load a new entry. Make sure that there is enough room in the cache first
                if (elementQueue.remainingCapacity() == 0) {
                    String evictedFilename = elementQueue.poll();
                    cache.remove(evictedFilename);
                }
                elementQueue.add(oldFilename);

                // Create & activate a future and return the result from outside of the synchronization block
                offsetMap = executor.submit(new CallableLoader(oldFilename));
                cache.put(oldFilename, offsetMap);
            }
        }

        // We have a Future which either has an offsetMap or will at some point have one (or fail)
        // get blocks, so we simply return the result from that
        try {
            return offsetMap.get();
        } catch (InterruptedException e) {
            FileNotFoundException notFound = new FileNotFoundException(
                    "Interrupted while waiting for loading of entry for '" + oldFilename + "'");
            notFound.initCause(e);
            throw notFound;
        } catch (ExecutionException e) {
            FileNotFoundException notFound = new FileNotFoundException(
                    "Internal error: ExecutionException while loading entry '" + oldFilename + "'");
            notFound.initCause(e);
            throw notFound;
        }
    }

    private class CallableLoader implements Callable<ConcurrentSkipListMap<Long, IFileEntry>> {
        private final String oldFileName;

        public CallableLoader(String oldFileName) {
            this.oldFileName = oldFileName;
        }

        @Override
        public ConcurrentSkipListMap<Long, IFileEntry> call() throws Exception {
            return iFileLoader.getIFileEntryMap(oldFileName);
        }
    }

}
