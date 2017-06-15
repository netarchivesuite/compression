package dk.nationalbiblioteket.netarkivet.compression.tools;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;

public class ValidateMetadataOutput {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Wrong number of arguments. Got " + args.length + " arguments. Expected 2");
            System.err.println("Usage: java ValidateMetadataOutput inputmetadata outputmetadata");
            System.exit(1);
        }
        File inputfile = new File(args[0]);
        File outputfile = new File(args[1]);
        if (!checkIsSameFormat(inputfile, outputfile)) {
            System.err.println("The two files(" + inputfile.getAbsolutePath() + "," + outputfile.getAbsolutePath() + ") are in different format. Exiting");
            System.exit(1); 
        }
        
        int inputfileCount = getRecordCount(inputfile); 
        int outputfileCount = getRecordCount(outputfile);
        int difference = outputfileCount - inputfileCount;
        if (difference != 2) {
            System.err.println("Expected a difference of 2, but the difference of records between  " + inputfile.getAbsolutePath() + " and " + outputfile.getAbsolutePath() + " was " + difference);
        }
    }

    private static boolean checkIsSameFormat(File inputfile, File outputfile) {
        if (inputfile.getName().toLowerCase().endsWith("warc.gz") && outputfile.getName().toLowerCase().endsWith("warc.gz")) {
            return true;
        } else if (inputfile.getName().toLowerCase().endsWith("arc.gz") && outputfile.getName().toLowerCase().endsWith("arc.gz")) {
            return true;
        }
        return false;
        
    }

    /**
     * Traverse an gzipped arc or warc file, and return the number of records
     * @param inputfile
     * @return 0 if not a gzipped arc or warc file
     */
    private static int getRecordCount(File inputfile) {
        int count=0;
        if (inputfile.getName().toLowerCase().endsWith("warc.gz")) {
            WARCReader ar = null;
            try {
                ar = WARCReaderFactory.get(inputfile);
                Iterator<ArchiveRecord> i = ar.iterator();
                while (i.hasNext()) {
                    i.next();
                    count++;
                }
            } catch(IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeQuietly(ar);
            }
            
        } else if (inputfile.getName().toLowerCase().endsWith("arc.gz")) {
            ARCReader ar = null;
            try {
                ar = ARCReaderFactory.get(inputfile);
                Iterator<ArchiveRecord> i = ar.iterator();
                while (i.hasNext()) {
                    i.next();
                    count++;
                }
            } catch(IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeQuietly(ar);
            }
        }
        return count;
    }
}
