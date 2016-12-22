package dk.nationalbiblioteket.netarkivet.compression.metadata;


import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.precompression.FatalException;
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

    private ArrayBlockingQueue<String> elementQueue;


    private ConcurrentHashMap<String, ConcurrentSkipListMap<Long, IFileEntry>> cache =
            new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, IFileEntry>>();


    public IFileCacheImpl(int size) {
        elementQueue = new ArrayBlockingQueue<String>(size);
    }


    private Map<Long,IFileEntry> loadFile(String filename) throws FileNotFoundException {
        synchronized (elementQueue) {
            ConcurrentSkipListMap<Long, IFileEntry> newMap = new ConcurrentSkipListMap<>();
            // Now actually load the map from the file
            File subdir = Util.getIFileSubdir(filename, false);
            File ifile = new File(subdir, filename + ".ifile.cdx");
            if (!ifile.exists()) {
                throw new FileNotFoundException("No such file: " + ifile.getAbsolutePath());
            }
            try(InputStream is = new FileInputStream(ifile)) {
                for (Object lineO: IOUtils.readLines(is) ){
                    String[] line = ((String) lineO).trim().split("\\s");
                    newMap.put(Long.parseLong(line[0]), new IFileEntry(Long.parseLong(line[1]), line[2]));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
}
