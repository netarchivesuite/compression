package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileCacheImpl;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileCacheSoftApacheImpl;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileLoader;

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

    // TODO: Collapse the two methods into 1 by rewriting objectbased to use the new interfaces
    public static IFileCache getIFileCache(IFileMapLoader iFileMapLoader) {
        // Note: Only soft references
        return IFileGenericCache.getInstance(iFileMapLoader);
    }
}
