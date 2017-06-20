package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trilong;


import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.apache.commons.io.IOUtils;

import java.io.*;
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
     * Loads a listmap from the given file. The format is {@code key value1 value2\n} where all values are represented
     * as written numbers.
     */
    public TriLongListMap(String filename) throws IOException {
        this.filename = filename;

        long[] keys = new long[1000];
        long[] values1 = new long[1000];
        long[] values2 = new long[1000];
        int index = 0;

        File subdir = Util.getIFileSubdir(filename, false);
        File ifile = new File(subdir, filename + ".ifile.cdx");
        if (!ifile.exists()) {
            throw new FileNotFoundException("No such file: " + ifile.getAbsolutePath());
        }
        try(InputStream is = new FileInputStream(ifile)) {
            for (Object lineO: IOUtils.readLines(is) ){ // TODO remove use of deprecated method
                // Make sure there is room
                if (index == keys.length) {
                    keys = extend(keys);
                    values1 = extend(values1);
                    values2 = extend(values2);
                }
                String[] line = ((String) lineO).trim().split("\\s");
                keys[index] = Long.parseLong(line[0]);
                values1[index] = Long.parseLong(line[1]);
                values2[index] = Long.parseLong(line[2]);
                index++;
            }
        }
        // Reduce arrays
        this.keys = reduce(keys, index);
        this.values1 = reduce(values1, index);
        this.values2 = reduce(values2, index);
    }

    /**
     * Creates a listmap directly from the provided values.
     */
    public TriLongListMap(String filename, long[] keys, long[] values1, long[] values2) {
        this.filename = filename;
        this.keys = keys;
        this.values1 = values1;
        this.values2 = values2;
    }

    private long[] extend(long[] elements) {
        final long[] newElements = new long[elements.length*2];
        System.arraycopy(elements, 0, newElements, 0, elements.length);
        return newElements;
    }

    private long[] reduce(long[] elements, int size) {
        if (size == elements.length) {
            return elements;
        }
        final long[] newElements = new long[size];
        System.arraycopy(elements, 0, newElements, 0, size);
        return newElements;
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
    protected abstract long objectToValue1(T object);
    protected abstract long objectToValue2(T object);

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
