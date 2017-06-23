package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import org.apache.commons.collections4.map.AbstractReferenceMap;
import org.apache.commons.collections4.map.ReferenceMap;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Handles hard- and soft-caching of IFileMaps.
 */
public class IFileGenericCache implements IFileCache {
    private IFileMapLoader iFileLoader;
    private ArrayBlockingQueue<IFileMap> hardCache;
    private Map<String, IFileMap> cache;

    private static IFileGenericCache instance;
    // TODO: It is really ugly to use a Singleton with an argument
    public static synchronized IFileGenericCache getInstance(IFileMapLoader iFileLoader) {
        if (instance == null) {
            instance = new IFileGenericCache(iFileLoader);
        }
        return instance;
    }
    public static void clearInstance() {
        instance = null;
    }

    protected IFileGenericCache(IFileMapLoader iFileLoader) {
        this.iFileLoader = iFileLoader;
        ReferenceMap<String, IFileMap> baseMap =
                new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD, AbstractReferenceMap.ReferenceStrength.SOFT, true);
        cache = Collections.synchronizedMap(baseMap);
        int hardCacheSize = Integer.parseInt(Util.getProperties().getProperty(Util.CACHE_SIZE));
        if (hardCacheSize > 0) {
            hardCache = new ArrayBlockingQueue<>(hardCacheSize);
        }
    }

    @Override
    public synchronized IFileEntry getIFileEntry(String oldFilename, Long oldOffset) throws FileNotFoundException {
        IFileMap ifileMap = loadFile(oldFilename);
        return ifileMap.get(oldOffset);
    }

    private IFileMap loadFile(String oldFilename) throws FileNotFoundException {
        IFileMap ifileMap = cache.get(oldFilename);
        if (ifileMap == null) {
            ifileMap = iFileLoader.getIFileMap(oldFilename);
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

    // TODO: Add estimated memory use
    @Override
    public String toString() {
        return "Cache size " + cache.size();
    }

}
