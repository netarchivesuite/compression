package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.AlreadyKnownMissingFileException;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 */
public class IFileLoaderImpl implements IFileLoader {

    static List<String> knownMissing = new ArrayList<>();

    @Override
    public ConcurrentSkipListMap<Long, IFileEntry> getIFileEntryMap(String filename) throws FileNotFoundException {
        synchronized (knownMissing) {
            if (knownMissing.contains(filename)) {
                throw new AlreadyKnownMissingFileException();
            }
        }
        ConcurrentSkipListMap<Long, IFileEntry> newMap = new ConcurrentSkipListMap<>();
        // Now actually load the map from the file
        File subdir = Util.getIFileSubdir(filename, false);
        File ifile = new File(subdir, filename + ".ifile.cdx");
        if (!ifile.exists()) {
            synchronized (knownMissing) {
                knownMissing.add(filename);
            }
            throw new FileNotFoundException("No such file: " + ifile.getAbsolutePath());
        }
        try(InputStream is = new FileInputStream(ifile)) {
            for (Object lineO: IOUtils.readLines(is) ){ // TODO remove use of deprecated method
                String[] line = ((String) lineO).trim().split("\\s");
                newMap.put(Long.parseLong(line[0]), new IFileEntry(Long.parseLong(line[1]), Long.parseLong(line[2])));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return newMap;
    }
}
