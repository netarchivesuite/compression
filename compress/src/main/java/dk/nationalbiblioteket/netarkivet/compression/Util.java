package dk.nationalbiblioteket.netarkivet.compression;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by csr on 12/22/16.
 */
public class Util {

    public static final String CONFIG = "config";
    public static final String IFILE_ROOT_DIR = "IFILE_ROOT_DIR";
    public static final String CDX_ROOT_DIR = "CDX_ROOT_DIR";
    public static final String IFILE_DEPTH = "IFILE_DEPTH";
    public static final String TEMP_DIR = "TEMP_DIR";
    public static final String OUTPUT_DIR = "OUTPUT_DIR";
    public static final String NMETADATA_DIR = "NMETADATA_DIR";
    public static final String METADATA_DIR = "METADATA_DIR";
    public static final String CACHE_SIZE = "CACHE_SIZE";
    public static final String METADATA_GENERATION = "METADATA_GENERATION";
    public static final String MD5_FILEPATH = "MD5_FILEPATH";
    public static final String LOG = "LOG";
    public static final String CDX_DEPTH = "CDX_DEPTH";
    public static final String THREADS = "THREADS";
    public static final String UPDATED_FILENAME_MD5_FILEPATH = "UPDATED_FILENAME_MD5_FILEPATH";
    public static final String COMPRESSION_LEVEL = "9";
    public static final String DRYRUN = "DRYRUN";

    private static final Pattern METADATA_NAME_PATTERN = Pattern.compile("([0-9]*-metadata-)([0-9]+)(.(w)?arc)(.gz)?");


    public static Properties properties;


    public static File getIFileSubdir(String uncompressedFileName, boolean create) {
        return getSubdir(properties.getProperty(IFILE_ROOT_DIR), uncompressedFileName, Integer.parseInt(properties.getProperty(IFILE_DEPTH)), create);
    }

    public static File getCDXSubdir(String uncompressedFileName, boolean create) {
        return getSubdir(properties.getProperty(CDX_ROOT_DIR), uncompressedFileName, Integer.parseInt(properties.getProperty(CDX_DEPTH)), create);
    }


    private static File getSubdir(String rootDir, String uncompressedFileName, int depth,  boolean create) {
        getProperties();
        String inputFilePrefix = uncompressedFileName.split("-")[0];
        while ( inputFilePrefix.length() < depth )  {
            inputFilePrefix = '0' + inputFilePrefix;
        }
        inputFilePrefix = inputFilePrefix.substring(0, depth);
        File subdir = new File(rootDir);
        subdir.mkdirs();
        for (char subdirName: inputFilePrefix.toCharArray()) {
            subdir = new File(subdir, String.valueOf(subdirName));
            if (create) {
                subdir.mkdirs();
            }
        }
        return subdir;
    }

    public static String getNewMetadataFilename(String oldFilepath) {
        String originalName = new File(oldFilepath).getName();
        Matcher m = METADATA_NAME_PATTERN.matcher(originalName);
        if (m.matches()) {
            return m.group(1) + properties.getProperty(METADATA_GENERATION) + m.group(3) + ".gz";
        } else {
            return null;
        }
    }

    public static synchronized Properties getProperties() {
        if (properties == null) {
            String configFile = System.getProperty(Util.CONFIG);
            properties = new Properties();
            try {
                properties.load(new FileInputStream(new File(configFile)));
            } catch (IOException e) {
                throw new PropertyException(e);
            }
        }
        return properties;
    }

}
