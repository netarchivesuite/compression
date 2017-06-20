package dk.nationalbiblioteket.netarkivet.compression.metadata;

import java.io.IOException;

/**
 * Note: This implementation requires the value inside of the IFileEntrys to be parsable as a long.
 */
public class IFileEntryMap extends TriLongListMap<IFileEntry> {

    public IFileEntryMap(String filename) throws IOException {
        super(filename);
    }

    public IFileEntryMap(String filename, long[] keys, long[] values1, long[] values2) {
        super(filename, keys, values1, values2);
    }

    @Override
    protected IFileEntry valuesToObject(long value1, long value2) {
        return new IFileEntry(value1, Long.toString(value2));
    }

    @Override
    protected long objectToValue1(IFileEntry object) {
        return object.getKey();
    }

    @Override
    protected long objectToValue2(IFileEntry object) {
        return Long.parseLong(object.getValue());
    }
}
