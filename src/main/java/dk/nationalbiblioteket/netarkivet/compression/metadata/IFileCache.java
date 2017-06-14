package dk.nationalbiblioteket.netarkivet.compression.metadata;


import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public interface IFileCache {

    IFileEntry getIFileEntry(String oldFilename, Long oldOffset) throws FileNotFoundException;

    Iterator<Map.Entry<Long, IFileEntry>> getOrderedListing(String oldFilename) throws FileNotFoundException;

    int getCurrentCachesize();

}