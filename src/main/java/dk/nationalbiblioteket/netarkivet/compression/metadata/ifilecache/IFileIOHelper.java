package dk.nationalbiblioteket.netarkivet.compression.metadata.ifilecache;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.apache.commons.io.IOUtils;

import java.io.*;

/**
 * Helper class for loading IFile-structures from the file system.
 */
public class IFileIOHelper {
    public static IFileArrays loadAsArrays(String filename) throws IOException {
        IFileArrays ifArrays = new IFileArrays();

        File subdir = Util.getIFileSubdir(filename, false);
        File ifile = new File(subdir, filename + ".ifile.cdx");
        if (!ifile.exists()) {
            throw new FileNotFoundException("No such file: " + ifile.getAbsolutePath());
        }
        try(InputStream is = new FileInputStream(ifile)) {
            for (Object lineO: IOUtils.readLines(is) ){ // TODO remove use of deprecated method
                String[] line = ((String) lineO).trim().split("\\s");
                ifArrays.add(Long.parseLong(line[0]),
                             Long.parseLong(line[1]),
                             Long.parseLong(line[2]));
            }
        }
        return ifArrays;
    }

    /**
     * Auto-extending {@code long[]}-based structure for IFile data.
     */
    public static class IFileArrays {
        private long[] keys = new long[1000];
        private long[] values1 = new long[1000];
        private long[] values2 = new long[1000];
        private int index = 0;

        public void add(long key, long val1, long val2) {
            if (index == keys.length) {
                keys = extend(keys);
                values1 = extend(values1);
                values2 = extend(values2);
            }
            keys[index] = key;
            values1[index] = val1;
            values2[index] = val2;
            index++;
        }

        /**
         * Trim the arrays to fit the content exactly.
         */
        public void reduce() {
            keys = reduce(keys, index);
            values1 = reduce(values1, index);
            values2 = reduce(values2, index);
        }

        public long[] getKeys() {
            return keys;
        }

        public long[] getValues1() {
            return values1;
        }

        public long[] getValues2() {
            return values2;
        }

        public int size() {
            return index;
        }

        public IFileArrays set(long[] keys, long[] values1, long[] values2, int size) {
            this.keys = keys;
            this.values1 = values1;
            this.values2 = values2;
            this.index = size;
            return this;
        }

        private static long[] extend(long[] elements) {
            final long[] newElements = new long[elements.length * 2];
            System.arraycopy(elements, 0, newElements, 0, elements.length);
            return newElements;
        }

        private static long[] reduce(long[] elements, int size) {
            if (size == elements.length) {
                return elements;
            }
            final long[] newElements = new long[size];
            System.arraycopy(elements, 0, newElements, 0, size);
            return newElements;
        }
    }
}
