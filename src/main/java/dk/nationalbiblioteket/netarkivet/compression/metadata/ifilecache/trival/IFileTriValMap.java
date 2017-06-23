package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trival;

import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.IFileMap;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.TriLongListMap;

public class IFileTriValMap extends TriValListMap<IFileEntry>  implements IFileMap {

    public IFileTriValMap(String filename, long[] keys, long[] values1, long[] values2) {
        super(filename, keys, values1, values2);
    }

    public IFileTriValMap(String filename, long[] keys, long[] values1, long[] values2, int elementCount) {
        super(filename, keys, values1, values2, elementCount);
    }

    @Override
    protected IFileEntry valuesToObject(long value1, long value2) {
        return new IFileEntry(value1, value2);
    }

    @Override
    protected long objectToValue1(IFileEntry object) {
        return object.getKey();
    }

    @Override
    protected long objectToValue2(IFileEntry object) {
        return object.getValue();
    }
}
