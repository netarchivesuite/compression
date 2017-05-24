package dk.nationalbiblioteket.netarkivet.compression.metadata;

import java.io.FileNotFoundException;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 */
public interface IFileLoader {
    ConcurrentSkipListMap<Long, IFileEntry> getIFileEntryMap(String filename) throws FileNotFoundException;
}
