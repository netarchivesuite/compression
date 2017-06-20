package dk.nationalbiblioteket.netarkivet.compression.metadata;

import java.io.FileNotFoundException;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 */
public interface IFileTriLongLoader {
    IFileEntryMap getIFileEntryMap(String filename) throws FileNotFoundException;
}
