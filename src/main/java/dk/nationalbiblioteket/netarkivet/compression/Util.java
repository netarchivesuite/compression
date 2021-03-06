package dk.nationalbiblioteket.netarkivet.compression;

import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by csr on 12/22/16.
 */
public class Util {

    static {
        SLF4JBridgeHandler.install();
    }

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
    public static final String NMETADATA_DEPTH = "NMETADATA_DEPTH";
    public static final String THREADS = "THREADS";
    public static final String UPDATED_FILENAME_MD5_FILEPATH = "UPDATED_FILENAME_MD5_FILEPATH";
    public static final String COMPRESSION_LEVEL = "9";
    public static final String DRYRUN = "DRYRUN";
    public static final String USE_SOFT_CACHE = "USE_SOFT_CACHE";

    private static final Pattern METADATA_NAME_PATTERN = Pattern.compile("(.*-)([0-9]+)(.(w)?arc)(.gz)?");
    public static final String RECORD_HEADER_MAXSIZE = "RECORD_HEADER_MAXSIZE";
    public static final String PAYLOAD_HEADER_MAXSIZE = "PAYLOAD_HEADER_MAXSIZE";
    


    public static Properties properties;


    public static File getIFileSubdir(String uncompressedFileName, boolean create) {
        return getSubdir(properties.getProperty(IFILE_ROOT_DIR), uncompressedFileName, Integer.parseInt(properties.getProperty(IFILE_DEPTH)), create);
    }

    public static File getCDXSubdir(String uncompressedFileName, boolean create) {
        return getSubdir(properties.getProperty(CDX_ROOT_DIR), uncompressedFileName, Integer.parseInt(properties.getProperty(CDX_DEPTH)), create);
    }

    public static File getNMetadataSubdir(String uncompressedFileName, boolean create) {
        return getSubdir(properties.getProperty(NMETADATA_DIR), uncompressedFileName, Integer.parseInt(properties.getProperty(NMETADATA_DEPTH, "0")), create);
    }


    private static File getSubdir(String rootDir, String uncompressedFileName, int depth,  boolean create) {
        getProperties();
        String inputFilePrefix = new StringBuilder(uncompressedFileName.split("-")[0]).reverse().toString();
        while ( inputFilePrefix.length() < depth )  {
            inputFilePrefix =  inputFilePrefix + '0';
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

    public static String getMemoryStats() {
        Runtime runtime = Runtime.getRuntime();
        int mb = 1024*1024;
        return "Memory max/total/free (mb) = " + runtime.maxMemory()/mb + "/" + runtime.totalMemory()/mb + "/" + runtime.freeMemory()/mb ;
    }
    
    public static synchronized void writeToFile(File file, String msg, int tries, long delay) throws DeeplyTroublingException {
        String errMsg;
        boolean done = false;
        for (int i = 0; !done && i <= tries ; i++) {
            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
                writer.println(msg);
                done = true;
            } catch (IOException e) {
                errMsg = "Warning: Error writing to file " + file.getAbsolutePath();
                if (i < tries) {
                    System.out.println(errMsg);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e1) {
                        throw new DeeplyTroublingException(e1);
                    }
                } else {
                    throw new DeeplyTroublingException(e);
                }
            }
        }
    }
    public static String getLogPath() {
        return Util.getProperties().getProperty(Util.LOG);
    }
    
    public static List<String> getFilteredList(String filelistFilename, List<String> blacklisted) throws IOException {
        return getAllLines(filelistFilename).stream().filter(predicate(blacklisted)).collect(Collectors.toList());
    }
    
    private static Predicate<String> predicate(List<String> blacklisted) {
        return p -> !p.isEmpty() && !blacklisted.contains(getFilename(p));
    }
    
    private static String getFilename(String p) {
        String retValue = Paths.get(p).toFile().getName();
        //System.out.println(retValue);
        return retValue;
    }

    public static List<String> getAllLines(String filename) throws IOException {
        return Files.readAllLines(Paths.get(filename));
    }

}
