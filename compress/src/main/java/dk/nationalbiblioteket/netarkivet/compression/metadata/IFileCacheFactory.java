package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.Util;

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
}
