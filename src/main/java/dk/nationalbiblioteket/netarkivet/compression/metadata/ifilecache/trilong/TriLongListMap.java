package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong;


import java.util.*;

/**
 * Specialized structure equivalent to Map<Long, Pair<Long, Long>>. This uses 3 long[] internally for
 * low memory overhead, at the cost of doing binary search for lookups.
 * The structure is a minimal implementation, expecting its content to be naturally ordered upon creation.
 */
public abstract class TriLongListMap<T> extends AbstractMap<Long, T> {
    private final String filename;
    private final long[] keys;
    private final long[] values1;
    private final long[] values2;

    /**
     * Creates a listmap directly from the provided values.
     */
    public TriLongListMap(String filename, long[] keys, long[] values1, long[] values2) {
        this.filename = filename;
        this.keys = keys;
        this.values1 = values1;
        this.values2 = values2;
    }

    /**
     * @return the filename for the persistent data that this structure represents.
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @return the object representing the two long values that matches the key or null if an extry does not exist.
     */
    public T get(long key) {
        int index = Arrays.binarySearch(keys, key);
        return index >= 0 ? valuesToObject(values1[index], values2[index]) : null;
    }

    /**
     * @return the object at the given position in the list.
     */
    public T getAtIndex(int index) {
        return valuesToObject(values1[index], values2[index]);
    }

    @Override
    public int size() {
        return keys.length;
    }

    protected abstract T valuesToObject(long value1, long value2);

    private final long constOverhead = 12+100+24*3;
    /**
     * @return the approximate memory use of this structure, as a sum of field overhead and elements.
     */
    public long getBytesUsed() {
        return constOverhead + 8*3*size();
    }


    /* Implements the needed methods form AbstractMap below */

    @Override
    public Set<Entry<Long, T>> entrySet() {
        final TriLongListMap<T> parent = this;
        return new AbstractSet<Entry<Long, T>>() {
            @Override
            public Iterator<Entry<Long, T>> iterator() {
                return new TriLongIterator(parent);
            }

            @Override
            public int size() {
                return keys.length;
            }
        };
    }

    @Override
    public boolean isEmpty() {
        return keys.length == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public T get(Object key) {
        return (key instanceof Long) ? get(((Long)key).longValue()) : null;
    }

    private class TriLongIterator implements Iterator<Entry<Long, T>> {
        private final TriLongListMap<T> triMap;
        int index = 0;
        public TriLongIterator(TriLongListMap<T> triMap) {
            this.triMap = triMap;
        }

        @Override
        public boolean hasNext() {
            return index < triMap.keys.length;
        }

        @Override
        public Entry<Long, T> next() {
            return new TriLongMapEntry(triMap.keys[index], triMap.getAtIndex(index++));
        }
    }

    private class TriLongMapEntry implements Map.Entry<Long, T> {
        private final Long key;
        private T value;

        public TriLongMapEntry(Long key, T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Long getKey() {
            return key;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public T setValue(T value) {
            T oldValue = this.value;
            this.value = value;
            return oldValue;
        }
    }
}
