package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache;

import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;

import java.util.Map;
import java.util.Set;

public interface IFileMap {
    String getFilename();
    IFileEntry get(long key);
    Set<Map.Entry<Long, IFileEntry>> entrySet();
    long getBytesUsed();
}
