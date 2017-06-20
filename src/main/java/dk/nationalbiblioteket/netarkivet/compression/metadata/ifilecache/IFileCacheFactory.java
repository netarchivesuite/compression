package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileCacheImpl;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileCacheSoftApacheImpl;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileLoader;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileCacheSoftLongArrays;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong.IFileTriLongLoader;

/**
 * Created by csr on 5/24/17.
 */
public class IFileCacheFactory {

    public static IFileCache getIFileCache(IFileLoader iFileLoader) {
        boolean soft = Boolean.parseBoolean(Util.getProperties().getProperty(Util.USE_SOFT_CACHE, "true"));
        if (soft) {
            return IFileCacheSoftApacheImpl.getIFileCacheSoftApacheImpl(iFileLoader);
        } else {
            return IFileCacheImpl.getIFileCacheImpl(iFileLoader);
        }
    }

    public static IFileCache getIFileCache(IFileTriLongLoader iFileTriLongLoader) {
        // Note: Only soft references
        return IFileCacheSoftLongArrays.getIFileCacheSoftApacheImpl(iFileTriLongLoader);
    }
}
