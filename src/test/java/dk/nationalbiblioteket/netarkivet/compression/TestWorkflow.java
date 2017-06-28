package dk.nationalbiblioteket.netarkivet.compression;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.compression.CompressorRunnable;
import dk.nationalbiblioteket.netarkivet.compression.metadata.MetadatafileGeneratorRunnable;
import dk.nationalbiblioteket.netarkivet.compression.precompression.PrecompressionRunnable;

public class TestWorkflow {
    
    public static void main(String[] args) throws IOException {
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, "SVC_TEST_COMPRESS/ifiles" );
        Util.properties.put(Util.CDX_ROOT_DIR, "SVC_TEST_COMPRESS/cdxes");
        Util.properties.put(Util.IFILE_DEPTH, "4");
        Util.properties.put(Util.CDX_DEPTH, "0");
        Util.properties.put(Util.MD5_FILEPATH,   "SVC_TEST_COMPRESS/output/checksum_CS.md5");
        Util.properties.put(Util.TEMP_DIR, "SVC_TEST_COMPRESS/output");
        Util.properties.put(Util.THREADS, "10");
        Util.properties.put(Util.LOG, "SVC_TEST_COMPRESS/output/log");
        Util.properties.put(Util.CACHE_SIZE, "100");
        Util.properties.put(Util.OUTPUT_DIR, "SVC_TEST_COMPRESS/OUTPUT");
        Util.properties.put(Util.NMETADATA_DIR, "SVC_TEST_COMPRESS/NMETADATA");
        Util.properties.put(Util.METADATA_DIR,  "/home/svc/compression/SVC_TEST_COMPRESS/");
        Util.properties.put(Util.METADATA_GENERATION, "4");
        Util.properties.put(Util.UPDATED_FILENAME_MD5_FILEPATH, "SVC_TEST_COMPRESS/MD5_updates");
        //public static final String COMPRESSION_LEVEL = "9";
        //public static final String DRYRUN = "DRYRUN";
        //public static final String USE_SOFT_CACHE = "USE_SOFT_CACHE";
        
        BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
        String prefix = "/home/svc/devel/compression/SVC_TEST_COMPRESS/";
        //sharedQueue.add(prefix + "6429-metadata-1.arc");
        //sharedQueue.add(prefix + "8936-metadata-1.arc");
        File md5file = new File("SVC_TEST_COMPRESS/output/checksum_CS.md5");
        if (!md5file.exists()) {
            md5file.createNewFile();
        }
        String[] elements = new String[]{
                "6500-247-20160704113624290-00000-sb-test-har-001.statsbiblioteket.dk.warc", "6500-metadata-1.warc"};
        for (String element: elements) {
            sharedQueue.add(prefix + element);
        }
        PrecompressionRunnable PR = new PrecompressionRunnable(sharedQueue, 1);
        new Thread(PR).start();
        
        CompressorRunnable CR = new CompressorRunnable(sharedQueue, 1);
        new Thread(CR).start();
        
        MetadatafileGeneratorRunnable MGR = new MetadatafileGeneratorRunnable(sharedQueue, 1);
        new Thread(MGR).start();
        
    }
}
