package dk.nationalbiblioteket.netarkivet.compression.metadata;

import java.io.FileNotFoundException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * .
 */
public class IFileCacheSoftImpl implements IFileCache {

    private static IFileCacheSoftImpl instance;

    private IFileLoader iFileLoader;

    private ReferenceQueue<ConcurrentSkipListMap<Long, IFileEntry>> referenceQueue = null;

    private ConcurrentHashMap<String, SoftReference<ConcurrentSkipListMap<Long, IFileEntry>>> cache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<SoftReference<ConcurrentSkipListMap<Long, IFileEntry>>, String> refs = new ConcurrentHashMap<>();

    public static synchronized IFileCacheSoftImpl getIFileCacheSoftImpl(IFileLoader iFileLoader) {
        if (instance == null) {
            instance = new IFileCacheSoftImpl(iFileLoader);
        }
        return instance;
    }

    private IFileCacheSoftImpl(IFileLoader iFileLoader) {
        this.iFileLoader = iFileLoader;
        referenceQueue = new ReferenceQueue<>();
        Thread cleanup = new Thread() {
            @Override
            public void run() {
                try {
                    for (;;) {
                        SoftReference<ConcurrentSkipListMap<Long, IFileEntry>> ref = (SoftReference<ConcurrentSkipListMap<Long, IFileEntry>>) referenceQueue.remove();
                        String key = refs.get(ref);
                        cache.remove(key);
                        refs.remove(ref);
                    }
                } catch (InterruptedException e) {
                }
            }
        };
        cleanup.setDaemon(true);
        cleanup.start();
    }


    @Override
    public synchronized IFileEntry getIFileEntry(String oldFilename, Long oldOffset) throws FileNotFoundException {
        ConcurrentSkipListMap<Long, IFileEntry> ifileMap = loadFile(oldFilename);
        return ifileMap.get(oldOffset);
    }

    private ConcurrentSkipListMap<Long, IFileEntry> loadFile(String oldFilename) throws FileNotFoundException {
        ConcurrentSkipListMap<Long, IFileEntry> ifileMap = null;
        SoftReference<ConcurrentSkipListMap<Long, IFileEntry>> ref = cache.get(oldFilename);
        if (ref != null) {
            ifileMap = ref.get();
        }
        if (ifileMap == null) {
            ifileMap = iFileLoader.getIFileEntryMap(oldFilename);
            cache.put(oldFilename, new SoftReference<ConcurrentSkipListMap<Long, IFileEntry>>(ifileMap, referenceQueue));
            refs.put(cache.get(oldFilename), oldFilename);
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
