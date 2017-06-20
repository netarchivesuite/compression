package dk.nationalbiblioteket.netarkivet.compression.metadata;

import java.util.AbstractMap;

/**
 * Represents the pair (newOffset, timestamp) in an ifile.
 */
public class IFileEntry extends AbstractMap.SimpleImmutableEntry<Long, Long> {
    public IFileEntry(Long key, Long value) {
        super(key, value);
    }

    public Long getNewOffset() {
        return getKey();
    }

    public Long getTimestamp() {
        return getValue();
    }
}
