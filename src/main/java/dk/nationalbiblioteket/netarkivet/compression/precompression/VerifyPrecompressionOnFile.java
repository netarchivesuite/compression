package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.netarkivet.common.utils.cdx.CDXRecord;
import dk.netarkivet.common.utils.cdx.CDXUtils;

public class VerifyPrecompressionOnFile {
    static final String SEPARATOR_REGEX = "\\s+";
    public static void main(String[] args) throws DeeplyTroublingException, WeirdFileException, IOException {
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
        File ifileSubdir = Util.getIFileSubdir(inputFile.getName(), false);
        File ifile = new File(ifileSubdir, inputFile.getName() + ".ifile.cdx");
        PrecompressionRunnable consumer = new PrecompressionRunnable(null, 0);
        consumer.precompress(inputFile.getAbsolutePath());
        if (ifile.exists()) {
            System.out.println("Dannet ifilen: " + ifile.getAbsolutePath() );
        }
        
        OutputStream cdxstream = null;
        File cdxfile = new File(inputFile.getParentFile(), inputFile.getName() + ".cdx");
        try {
            cdxstream = new FileOutputStream(cdxfile);
            CDXUtils.writeCDXInfo(inputFile, cdxstream);
        } finally {
            if (cdxstream != null) {
                cdxstream.close();
            }
        }
        if (ifile.exists()) {
            System.out.println("Dannet ifilen: " + ifile.getAbsolutePath() );
        }
        
        if (cdxfile.exists()) {
            System.out.println("Dannet cdxfilen: " + cdxfile.getAbsolutePath() );
        }
        System.out.println("Comparing generated ifile with cdxfile ....");
        List<String> ifiles = FileUtils.readLines(ifile);
        System.out.println("Read " + ifiles.size() + " ifilelines from ifile");
        Set<Long> offsets = new HashSet<Long>();
        int i1 = 0;
        for (String i: ifiles) {
            i1++;
            String[] splitLine = StringUtils.split(i);
            //System.out.println("# " + i1 + ": " + splitLine[0]);
            Long offset = Long.valueOf(splitLine[0]);
            offsets.add(offset);
        }
        List<CDXRecord> records = getRecords(cdxfile);
        System.out.println("Read " + records.size() + " cdxrecords");
        System.out.println("Read " + offsets.size() + " offsets from ifile");
        int missing=0;
        int found=0;
        for (CDXRecord r: records) {
            Long off = r.getOffset();
            if (offsets.contains(off)) {
                found++;
            } else {
                missing++;
            }
        }
        System.out.println("Of the " + records.size() + " cdxrecords generated, we found " 
                +  found + " in the ifile. Missing is " +  missing);
        
    }
    
    public static List<CDXRecord> getRecords(File cdxFile) throws IOException {
        List<CDXRecord> entries = new ArrayList<CDXRecord>();
        List<String> cdxes = FileUtils.readLines(cdxFile);
        for (String cdx: cdxes) {
            String[] fieldParts = cdx.split(SEPARATOR_REGEX);
            CDXRecord cdxrec = null;
            try {
                cdxrec = new CDXRecord(fieldParts);
                entries.add(cdxrec);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        return entries;
    }

}
