package dk.nationalbiblioteket.netarkivet.compression;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by csr on 12/22/16.
 */
public class Util {

    public static final String CONFIG = "config";
    public static final String IFILE_ROOT_DIR = "IFILE_ROOT_DIR";
    public static final String DEPTH = "DEPTH";
    public static final String TEMP_DIR = "TEMP_DIR";
    public static final String NMETADATA_DIR = "NMETADATA_DIR";
    public static final String METADATA_DIR = "METADATA_DIR";
    public static final String CACHE_SIZE = "CACHE_SIZE";
    public static final String METADATA_GENERATION = "METADATA_GENERATION";
    private static final Pattern METADATA_NAME_PATTERN = Pattern.compile("([0-9]*-metadata-)([0-9]+)(.(w)?arc)");
    public static Properties properties;


    public static File getIFileSubdir(String uncompressedFileName, boolean create) {
        getProperties();
        String outputRootDirName = properties.getProperty(IFILE_ROOT_DIR);
        String inputFilePrefix = uncompressedFileName.split("-")[0];
        int depth = Integer.parseInt(properties.getProperty(DEPTH));
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
