package dk.nationalbiblioteket.netarkivet.compression.metadata;

import java.util.AbstractMap;

/**
 * Represents the pair (newOffset, timestamp) in an ifile.
 */
public class IFileEntry extends AbstractMap.SimpleImmutableEntry<Long, String> {
    public IFileEntry(Long key, String value) {
        super(key, value);
    }

    public Long getNewOffset() {
        return getKey();
    }

   public String getTimestamp() {
       return getValue();
   }
}
