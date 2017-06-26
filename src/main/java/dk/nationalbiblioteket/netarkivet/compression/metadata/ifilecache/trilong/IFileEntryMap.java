package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong;

import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileMap;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;

import java.io.IOException;

/**
 * Note: This implementation requires the value inside of the IFileEntrys to be parsable as a long.
 */
public class IFileEntryMap extends TriLongListMap<IFileEntry> implements IFileMap {

    public IFileEntryMap(String filename, long[] keys, long[] values1, long[] values2) {
        super(filename, keys, values1, values2);
    }

    @Override
    protected IFileEntry valuesToObject(long value1, long value2) {
        return new IFileEntry(value1, value2);
    }

}
