package dk.nationalbiblioteket.netarkivet.compression.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import dk.nationalbiblioteket.netarkivet.compression.metadata.MetadatafileGeneratorRunnable;
import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;

import dk.netarkivet.common.utils.FileUtils;
import dk.netarkivet.common.utils.archive.GetMetadataArchiveBatchJob;
import dk.netarkivet.common.utils.batch.BatchLocalFiles;
import dk.netarkivet.common.utils.batch.FileBatchJob;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidateMetadataOutput {

    static Logger logger = LoggerFactory.getLogger(ValidateMetadataOutput.class);


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

    public static int getRecordDiff(File originalMetadataFile,
            File newMetadataFile) {
        return getRecordCount(newMetadataFile) - getRecordCount(originalMetadataFile);
    }
    
    public static int findDuplicateLinesInCrawlog(File originalMetadataFile) throws IOException {
        BatchLocalFiles files = new BatchLocalFiles(new File[]{originalMetadataFile});
        File resultFile = File.createTempFile("batch", ".txt");
        OutputStream os = new FileOutputStream(resultFile);
        Pattern CrawlLogUrlpattern = Pattern.compile(MetadataFile.CRAWL_LOG_PATTERN);
        Pattern textPlainMimepattern = Pattern.compile("text/plain");

        FileBatchJob job = new GetMetadataArchiveBatchJob(CrawlLogUrlpattern, textPlainMimepattern); 
        files.run(job, os);
        //Find duplicate annotations in log
        BufferedReader br = null;
        int duplicateLines = 0;
        try {
            br = new BufferedReader(new FileReader(resultFile));
            String line = null;  
            while ((line = br.readLine()) != null)  {  
                if (line.contains("duplicate:")) {
                    duplicateLines++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(br);
            resultFile.delete();
        }
        return duplicateLines;
    }
    
    /**
     * 
     * @param originalMetadataFile
     * @param newMetadataFile
     * @param dedupCdxFile
     * @return
     * @throws IOException 
     */
    public static boolean compareCrawllogWithDedupcdxfile(File originalMetadataFile, File newMetadataFile, File dedupCdxFile) throws IOException {
        int duplicateLinesInOriginal = findDuplicateLinesInCrawlog(originalMetadataFile);
        //System.out.println("crawlog duplicateLines in original: " + duplicateLinesInOriginal);
        int duplicateLinesInNew = findDuplicateLinesInCrawlog(newMetadataFile);
        //System.out.println("crawlog duplicateLines in new: " + duplicateLinesInNew);
        if (duplicateLinesInNew != duplicateLinesInOriginal){
            logger.warn("Crawlog duplicateLines in original and in new metadata file is not identical. Original file '"
                    + originalMetadataFile.getAbsolutePath() + "' has #lines=" + duplicateLinesInOriginal 
                    + ", but new file '" + newMetadataFile.getAbsolutePath() + "' has #lines= " +  duplicateLinesInNew);
        }
        
        List<String> lines = FileUtils.readListFromFile(dedupCdxFile);
        if (lines.size() != duplicateLinesInNew) {
            logger.warn("Crawlog duplicateLines (" + duplicateLinesInNew + ") in file '" + newMetadataFile.getAbsolutePath()
                    + "' does not match # lines in dedupCdxFile '" + dedupCdxFile.getAbsolutePath()  + "': " + lines.size());
            return false;
        }
        
        return true;
    }
    
}
