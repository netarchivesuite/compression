package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileCache;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import org.apache.commons.collections4.map.AbstractReferenceMap;
import org.apache.commons.collections4.map.ReferenceMap;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by csr on 5/22/17.
 */
public class IFileCacheSoftLongArrays implements IFileCache {

    private static IFileCacheSoftLongArrays instance;

    private IFileTriLongLoader iFileLoader;

    private static ArrayBlockingQueue<IFileEntryMap> hardCache;

    private static Map<String, IFileEntryMap> cache;

    public static synchronized IFileCacheSoftLongArrays getIFileCacheSoftApacheImpl(IFileTriLongLoader iFileLoader) {
        if (instance == null) {
            instance = new IFileCacheSoftLongArrays(iFileLoader);
        }
        return instance;
    }

    private IFileCacheSoftLongArrays(IFileTriLongLoader iFileLoader) {
        this.iFileLoader = iFileLoader;
        ReferenceMap<String, IFileEntryMap> baseMap =
                new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD, AbstractReferenceMap.ReferenceStrength.SOFT, true);
        cache = Collections.synchronizedMap(baseMap);
        int hardCacheSize = Integer.parseInt(Util.getProperties().getProperty(Util.CACHE_SIZE));
        if (hardCacheSize > 0) {
            hardCache = new ArrayBlockingQueue<>(hardCacheSize);
        }
    }


    @Override
    public synchronized IFileEntry getIFileEntry(String oldFilename, Long oldOffset) throws FileNotFoundException {
        IFileEntryMap ifileMap = loadFile(oldFilename);
        return ifileMap.get(oldOffset);
    }

    private IFileEntryMap loadFile(String oldFilename) throws FileNotFoundException {
        IFileEntryMap ifileMap = cache.get(oldFilename);
        if (ifileMap == null) {
            ifileMap = iFileLoader.getIFileEntryMap(oldFilename);
            synchronized (cache) {
                cache.put(oldFilename, ifileMap);
                if (hardCache != null) {
                    if (hardCache.remainingCapacity() == 0) {
                        hardCache.poll();
                    }
                    hardCache.add(ifileMap);
                }
            }
        }
        return ifileMap;
    }

    @Override
    public synchronized Iterator<Map.Entry<Long, IFileEntry>> getOrderedListing(String oldFilename) throws FileNotFoundException {
        return loadFile(oldFilename).entrySet().iterator();
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
