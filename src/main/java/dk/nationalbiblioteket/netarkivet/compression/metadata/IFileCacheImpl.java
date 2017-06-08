package dk.nationalbiblioteket.netarkivet.compression.metadata;


import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 */
public class IFileCacheImpl implements IFileCache {

    private static IFileCacheImpl instance;

    private IFileLoader iFileLoader;

    private ArrayBlockingQueue<String> elementQueue;


    private ConcurrentHashMap<String, ConcurrentSkipListMap<Long, IFileEntry>> cache =
            new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, IFileEntry>>();


    private IFileCacheImpl(IFileLoader iFileLoader) {
        int cacheSize = Integer.parseInt(Util.getProperties().getProperty(Util.CACHE_SIZE));
        elementQueue = new ArrayBlockingQueue<String>(cacheSize);
        this.iFileLoader = iFileLoader;
    }

    public static synchronized IFileCacheImpl getIFileCacheImpl(IFileLoader iFileLoader) {
        if (instance == null) {
            instance = new IFileCacheImpl(iFileLoader);
        }
        return instance;
    }


    private Map<Long,IFileEntry> loadFile(String filename) throws FileNotFoundException {
        synchronized (elementQueue) {
            ConcurrentSkipListMap<Long, IFileEntry> newMap = iFileLoader.getIFileEntryMap(filename);
            if (elementQueue.remainingCapacity() == 0) {
                String evictedFilename = elementQueue.poll();
                cache.remove(evictedFilename);
            }
            elementQueue.add(filename);
            cache.put(filename, newMap);
            return newMap;
        }
    }


    @Override
    public IFileEntry getIFileEntry(String oldFilename, Long oldOffset) throws FileNotFoundException {
        synchronized (elementQueue) {
            final ConcurrentSkipListMap<Long, IFileEntry> offsetMap = cache.get(oldFilename);
            if (offsetMap == null) {
                return loadFile(oldFilename).get(oldOffset);
            } else {
                return offsetMap.get(oldOffset);
            }
        }
    }

    @Override
    public Iterator<Map.Entry<Long, IFileEntry>> getOrderedListing(String oldFilename) throws FileNotFoundException {
        synchronized (elementQueue) {
            final ConcurrentSkipListMap<Long, IFileEntry> offsetMap = cache.get(oldFilename);
            if (offsetMap == null) {
                return loadFile(oldFilename).entrySet().iterator();
            } else {
                return offsetMap.entrySet().iterator();
            }
        }
    }

    @Override
    public int getCurrentCachesize() {
        return cache.size();
    }
}
