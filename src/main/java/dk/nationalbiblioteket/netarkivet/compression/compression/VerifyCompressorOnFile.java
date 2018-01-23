package dk.nationalbiblioteket.netarkivet.compression.compression;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.bridge.SLF4JBridgeHandler;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;

public class VerifyCompressorOnFile {

	public static void main(String[] args) throws DeeplyTroublingException, WeirdFileException, IOException {
    	SLF4JBridgeHandler.install();
    	if (args.length != 1) {
            System.err.println("Missing arg: uncompressed (w)arc file");
            System.exit(1);
        }
        File inputFile = new File(args[0]);
        if (!inputFile.exists()) {
            System.err.println("Given uncompressed (w)arc file '" + inputFile.getAbsolutePath() 
                    + "' does not exist");
            System.exit(1);
        }
        Util.properties = new Properties();
        Util.properties.put(Util.IFILE_ROOT_DIR, "ifiles" );
        Util.properties.put(Util.CDX_ROOT_DIR, "cdxes");
        Util.properties.put(Util.IFILE_DEPTH, "4");
        Util.properties.put(Util.CDX_DEPTH, "0");
        Util.properties.put(Util.MD5_FILEPATH,   "output/checksum_CS.md5");
        Util.properties.put(Util.TEMP_DIR, "output");
        Util.properties.put(Util.THREADS, "10");
        Util.properties.put(Util.LOG, "output/log");
        Util.properties.put(Util.CACHE_SIZE, "1000");
        Util.properties.put(Util.PAYLOAD_HEADER_MAXSIZE, (64 * 1024) + "");
        Util.properties.put(Util.RECORD_HEADER_MAXSIZE, (64 * 1024) + "");
        
        CompressorRunnable consumer = new CompressorRunnable(null, 0);
        consumer.compress(inputFile.getAbsolutePath());
    }
    
}
