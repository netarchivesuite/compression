package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache;

import java.io.FileNotFoundException;

// TODO: With Java 1.8 this is not really needed...
public interface IFileMapLoader {
    IFileMap getIFileMap(String filename) throws FileNotFoundException;
}
