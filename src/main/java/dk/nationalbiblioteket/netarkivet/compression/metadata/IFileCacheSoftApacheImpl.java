package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.apache.commons.collections4.map.AbstractReferenceMap;
import org.apache.commons.collections4.map.ReferenceMap;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by csr on 5/22/17.
 */
public class IFileCacheSoftApacheImpl implements IFileCache {

    private static IFileCacheSoftApacheImpl instance;

    private IFileLoader iFileLoader;

    private ArrayBlockingQueue<ConcurrentSkipListMap<Long, IFileEntry>> hardCache;

    private static Map<String, ConcurrentSkipListMap<Long, IFileEntry>> cache;

    public static synchronized IFileCacheSoftApacheImpl getIFileCacheSoftApacheImpl(IFileLoader iFileLoader) {
        if (instance == null) {
            instance = new IFileCacheSoftApacheImpl(iFileLoader);
        }
        return instance;
    }

    private IFileCacheSoftApacheImpl(IFileLoader iFileLoader) {
        this.iFileLoader = iFileLoader;
        ReferenceMap<String, ConcurrentSkipListMap<Long, IFileEntry>> baseMap =
                new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD, AbstractReferenceMap.ReferenceStrength.SOFT, true);
        cache = Collections.synchronizedMap(baseMap);
        int hardCacheSize = Integer.parseInt(Util.getProperties().getProperty(Util.CACHE_SIZE));
        hardCache = new ArrayBlockingQueue<>(hardCacheSize);
    }


    @Override
    public synchronized IFileEntry getIFileEntry(String oldFilename, Long oldOffset) throws FileNotFoundException {
        ConcurrentSkipListMap<Long, IFileEntry> ifileMap = loadFile(oldFilename);
        return ifileMap.get(oldOffset);
    }

    private ConcurrentSkipListMap<Long, IFileEntry> loadFile(String oldFilename) throws FileNotFoundException {
        ConcurrentSkipListMap<Long, IFileEntry> ifileMap = cache.get(oldFilename);
        if (ifileMap == null) {
            ifileMap = iFileLoader.getIFileEntryMap(oldFilename);
            synchronized (cache) {
                cache.put(oldFilename, ifileMap);
                if (hardCache.remainingCapacity() == 0) {
                    hardCache.poll();
                }
                hardCache.add(ifileMap);
            }
        }
        return ifileMap;
    }

    @Override
    public synchronized Iterator<Map.Entry<Long, IFileEntry>> getOrderedListing(String oldFilename) throws FileNotFoundException {
        ConcurrentSkipListMap<Long, IFileEntry> ifileMap = loadFile(oldFilename);
        return ifileMap.entrySet().iterator();
    }

    @Override
    public int getCurrentCachesize() {
        return cache.size();
    }

    @Override
    public String toString() {
        return "Cache size " + cache.size();
    }

}
