package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong;

import java.io.FileNotFoundException;

/**
 *
 */
public interface IFileTriLongLoader {
    IFileEntryMap getIFileEntryMap(String filename) throws FileNotFoundException;
}
