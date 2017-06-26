package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trival;


import java.util.*;

/**
 * Specialized structure equivalent to non-mutable Map<Long, Pair<Long, Long>>, representing key, value1 and value2
 * in the natural order of the key.
 *
 * Internally all elements are stored in a single {@code long[]}, with each element represented with an amount
 * of bits determined as {@code log2(max_val-min_val)} for all three parts.
 *
 * Further compression could be achieved by taking advantage of the ordering of the keys and storing deltas,
 * but that would likely have an impact on lookup-time.
 *
 * The structure is a minimal implementation, expecting its content to be naturally ordered upon creation.
 * Heavily influenced by Lucene/Solr Packed64.
 */
public abstract class TriValListMap<T> extends AbstractMap<Long, T> {
    static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
    static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
    static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

    private final String filename;

    private final long[] elements;
    private final int elementCount;

    private final int keyBits;
    private final int val1Bits;
    private final int val2Bits;
    private final int elementBits; // keyBits + val1Bits + val2Bits

    private final long keyOrigo;  // Minimum value
    private final long val1Origo; // Minimum value
    private final long val2Origo; // Minimum value

    /**
     * Creates a listmap from the provided values.
     */
    public TriValListMap(String filename, long[] keys, long[] values1, long[] values2) {
        this(filename, keys, values1, values2, keys.length);
    }

    /**
     * Creates a listmap from the provided values.
     */
    public TriValListMap(String filename, long[] keys, long[] values1, long[] values2, int elementCount) {
        this.filename = filename;
        keyOrigo = min(keys, elementCount);
        keyBits = bitsRequired(max(keys, elementCount)-keyOrigo);
        val1Origo = min(values1, elementCount);
        val1Bits = bitsRequired(max(values1, elementCount)-val1Origo);
        val2Origo = min(values2, elementCount);
        val2Bits = bitsRequired(max(values2, elementCount)-val2Origo);
        elementBits = keyBits + val1Bits + val2Bits;

        this.elementCount = elementCount;
        elements = new long[elementCount*elementBits/64+1];

        long previousKey = Long.MIN_VALUE;
        for (int i = 0 ; i < elementCount ; i++) {
            if (keys[i] <= previousKey) {
                throw new IllegalArgumentException(
                        "Element #" + i + " has key value " + keys[i] + ", with the previous key being " +
                        previousKey + ". The keys must be monotonically increasing");
            }
            previousKey = keys[i];
            set(i, keys[i], values1[i], values2[i]);
        }
    }

    private void set(int index, long key, long value1, long value2) {
        final int origoBitIndex = index*elementBits;
        setAbsolute(origoBitIndex, key - keyOrigo, keyBits);
        setAbsolute(origoBitIndex + keyBits, value1 - val1Origo, val1Bits);
        setAbsolute(origoBitIndex + keyBits + val1Bits, value2 - val2Origo, val2Bits);
    }

    private void setAbsolute(long majorBitPos, long value, int valBits) {
        final int elementPos = (int)(majorBitPos >>> BLOCK_BITS);
        // The number of value-bits in the second long
        final long endBits = (majorBitPos & MOD_MASK) + (valBits - BLOCK_SIZE);
        // TODO: Cache the maskRights for the three possibilities
        final long maskRight = ~0L << (BLOCK_SIZE-valBits) >>> (BLOCK_SIZE-valBits);

        if (endBits <= 0) { // Single block
            elements[elementPos] = elements[elementPos] &  ~(maskRight << -endBits)
                                   | (value << -endBits);
            return;
        }
        // Two elements
        elements[elementPos] = elements[elementPos] &  ~(maskRight >>> endBits)
                               | (value >>> endBits);
        elements[elementPos+1] = elements[elementPos+1] &  (~0L >>> endBits)
                                 | (value << (BLOCK_SIZE - endBits));
    }

    public long getKeyAtIndex(int index) {
        return getAbsolute(index*elementBits, keyBits)+keyOrigo;
    }
    public long getValue1AtIndex(int index) {
        return getAbsolute(index*elementBits+keyBits, val1Bits)+val1Origo;
    }
    public long getValue2AtIndex(int index) {
        return getAbsolute(index*elementBits+keyBits+val1Bits, val2Bits)+val2Origo;
    }

    private long getAbsolute(long majorBitPos, int valBits) {
        // The index in the backing long-array
        final int elementPos = (int)(majorBitPos >>> BLOCK_BITS);
        // The number of value-bits in the second long
        final long endBits = (majorBitPos & MOD_MASK) + (valBits - BLOCK_SIZE);
        // TODO: Cache the maskRights for the three possibilities
        final long maskRight = ~0L << (BLOCK_SIZE-valBits) >>> (BLOCK_SIZE-valBits);

        if (endBits <= 0) { // Single block
            return (elements[elementPos] >>> -endBits)
                   & maskRight;
        }
        // Two blocks
        return ((elements[elementPos] << endBits) | (elements[elementPos+1] >>> (BLOCK_SIZE - endBits)))
               & maskRight;
    }

    protected long min(long[] values, int valueCount) {
        if (valueCount == 0) {
            return 0;
        }
        long min = Long.MAX_VALUE;
        for (int i = 0; i < valueCount; i++) {
            if (values[i] < min) {
                min = values[i];
            }
        }
        return min;
    }
    protected long max(long[] values, int valueCount) {
        if (values.length == 0) {
            return 0;
        }
        long max = Long.MIN_VALUE;
        for (int i = 0; i < valueCount; i++) {
            if (values[i] > max) {
                max = values[i];
            }
        }
        return max;
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
        int index = binarySearch(key);
        return index >= 0 ? valuesToObject(getValue1AtIndex(index), getValue2AtIndex(index)) : null;
    }

    /**
     * @return the object at the given position in the list.
     */
    public T getAtIndex(int index) {
        // TODO: Collapse the multi-calls into 1
        return valuesToObject(getValue1AtIndex(index), getValue2AtIndex(index));
    }

    @Override
    public int size() {
        return elementCount;
    }

    protected abstract T valuesToObject(long value1, long value2);

    /* Implements the needed methods form AbstractMap below */

    @Override
    public Set<Entry<Long, T>> entrySet() {
        final TriValListMap<T> parent = this;
        return new AbstractSet<Entry<Long, T>>() {
            @Override
            public Iterator<Entry<Long, T>> iterator() {
                return new TriValIterator(parent);
            }

            @Override
            public int size() {
                return elementCount;
            }
        };
    }

    @Override
    public boolean isEmpty() {
        return elementCount == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public T get(Object key) {
        return (key instanceof Long) ? get(((Long)key).longValue()) : null;
    }

    private class TriValIterator implements Iterator<Entry<Long, T>> {
        private final TriValListMap<T> triMap;
        int index = 0;
        public TriValIterator(TriValListMap<T> triMap) {
            this.triMap = triMap;
        }

        @Override
        public boolean hasNext() {
            return index < triMap.elementCount;
        }

        @Override
        public Entry<Long, T> next() {
            return new TriValMapEntry(triMap.getKeyAtIndex(index), triMap.getAtIndex(index++));
        }
    }

    private class TriValMapEntry implements Entry<Long, T> {
        private final Long key;
        private T value;

        public TriValMapEntry(Long key, T value) {
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

    /**
     * Returns how many bits are required to hold values up to and including maxValue.
     * Copied from Lucene/Solr PackedInts
     * @param maxValue the maximum value that should be representable.
     * @return the amount of bits needed to represent values from 0 to maxValue.
     */
    private int bitsRequired(long maxValue) {
        if (maxValue < 0) {
            throw new IllegalArgumentException("maxValue must be non-negative (got: " + maxValue + ")");
        }
        return Math.max(1, 64 - Long.numberOfLeadingZeros(maxValue));
    }

    // From Arrays.binarySearch
    private int binarySearch(long key) {
        int low = 0;
        int high = elementCount - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = getKeyAtIndex(mid);

            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    private final long constOverhead = 12+100+24+12+5*4+3*8;
    /**
     * @return the approximate memory use of this structure, as a sum of field overhead and elements.
     */
    public long getBytesUsed() {
        return constOverhead + 8*elements.length;
    }

    @Override
    public String toString() {
        return String.format("TriValListMap(#elements=%d, " +
                             "bitsPerElement=(keyBits=%d, value1Bits=%d, value2Bits=%d)=%d, " +
                             "mem~=(constant %d + 8*#elements)=%d bytes), TriVal/TriLong~=%.2f%%",
                             elementCount, keyBits, val1Bits, val2Bits, keyBits+val1Bits+val2Bits,
                             constOverhead, getBytesUsed(), 100.0*(8* elements.length)/(24*elementCount));
    }
}
