package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.objectbased.IFileEntry;
import dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache.trival.IFileTriValMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class IFileTriValMapTest {

    @Test
    public void testBasicMap() throws Exception {
        IFileTriValMap map = createMap(
                1, 11, 101,
                2, 21, 201,
                3, 31, 301,
                5, 51, 501,
                8, 81, 801
        );
        assertEquals(Long.valueOf(51), map.get(5).getNewOffset(),
                     "Looking up key 5 should give the expected new offset");
        assertEquals(Long.valueOf(801), map.get(8).getTimestamp(),
                     "Looking up key 8 should give the expected timestamp");
    }

    @Test
    public void testBitBoundaries() throws Exception {
        final long val28 = 0b1111_11111111_11111111_11111111;
        IFileTriValMap map = createMap(
                1, 11, 0x0,
                val28<<3, val28<<3, val28,
                (val28<<3)+1, 30, 0
        );
        System.out.println("Created test map " + map);
        assertEquals(map.get(1).getTimestamp(), Long.valueOf(0x0),
                     "Looking up key 1 should give the expected timestamp");
        assertEquals(map.get(val28<<3).getTimestamp(), Long.valueOf(val28),
                     "Looking up key val28<<3 (" + (val28<<3) + ") should give the expected timestamp");
    }

    @Test
    public void testValuesIteration() {
        IFileTriValMap map = createMap(
                1, 11, 101,
                2, 21, 201,
                3, 31, 301
        );
        int index = 1;
        for (IFileEntry entry: map.values()) {
            assertEquals(Long.valueOf(index*10+1), entry.getNewOffset(),
                         "Value #" + (index-1) + " should have the expected new offset");
            assertEquals(Long.valueOf(index*100+1), entry.getTimestamp(),
                         "Value #" + (index-1) + " should have the expected timestamp");
            index++;
        }
    }

    private IFileTriValMap createMap(long... elements) {
        if (elements.length % 3 != 0) {
            throw new IllegalArgumentException("The number of elements must be divisible by 0");
        }
        int elementCount = elements.length/3;
        long[] keys = new long[elementCount];
        long[] values1 = new long[elementCount];
        long[] values2 = new long[elementCount];
        for (int i = 0 ; i < elementCount ; i++) {
            keys[i] = elements[i*3];
            values1[i] = elements[i*3+1];
            values2[i] = elements[i*3+2];
        }
        return new IFileTriValMap("dummy", keys, values1, values2);
    }

}