package dk.nationalbiblioteket.netarkivet.compression;

import dk.nationalbiblioteket.netarkivet.compression.precompression.PreCompressor;

import java.io.File;

/**
 * Created by csr on 12/22/16.
 */
public class Util {

    public static File getIFileSubdir(String uncompressedFileName, boolean create) {
        String outputRootDirName = PreCompressor.properties.getProperty(PreCompressor.OUTPUT_ROOT_DIR);
        String inputFilePrefix = uncompressedFileName.split("-")[0];
        int depth = Integer.parseInt(PreCompressor.properties.getProperty(PreCompressor.DEPTH));
        while ( inputFilePrefix.length() < depth )  {
                    inputFilePrefix = '0' + inputFilePrefix;
                }
                inputFilePrefix = inputFilePrefix.substring(0, depth);
                File subdir = new File(outputRootDirName);
                for (char subdirName: inputFilePrefix.toCharArray()) {
                    subdir = new File(subdir, String.valueOf(subdirName));
                    if (create) {
                        subdir.mkdirs();
                    }
                }
        return subdir;
    }

}
