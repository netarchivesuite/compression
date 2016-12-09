package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.jwat.tools.tasks.cdx.CDXOptions;
import org.jwat.tools.tasks.cdx.CDXTask;
import org.jwat.tools.tasks.compress.CompressFile;
import org.jwat.tools.tasks.compress.CompressOptions;
import org.jwat.tools.tasks.compress.CompressResult;
import org.jwat.tools.tasks.compress.CompressTask;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * Created by csr on 12/8/16.
 */
public class Consumer  extends CompressFile implements Runnable {

    private final BlockingQueue<String> sharedQueue;
    private int threadNo;

    public Consumer (BlockingQueue<String> sharedQueue, int threadNo) {
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
    }

    public void precompress(String filename) throws FatalException {
        String outputRootDirName = PreCompressor.propertiesMap.get("OUTPUT_ROOT_DIR");
        File outputRootDir = new File(outputRootDirName);
        File inputFile = new File(filename);
        File subdir = createAndGetOutputDir(outputRootDir, inputFile.getName().split("-")[0]);
        File outputFile = new File(subdir, inputFile.getName() + ".ifile.cdx");
        if (outputFile.exists()) {
            //already done this one
            return;
        }
        String osha1;
        try {
            osha1 = DigestUtils.sha1Hex(new FileInputStream(inputFile));
        } catch (java.io.IOException e) {
            throw new FatalException(e);
        }
        CompressOptions compressOptions = new CompressOptions();
        compressOptions.dstPath = subdir;
        this.compressFile(inputFile, compressOptions);
        File gzipFile = new File (subdir, inputFile.getName() + ".gz");
        String nsha1;
        try {
            nsha1 = DigestUtils.sha1Hex(new GZIPInputStream(new FileInputStream(gzipFile)));
        } catch (IOException e) {
            throw new FatalException(e);
        }
        if (!nsha1.equals(osha1)) {
            throw new FatalException("Checksum mismatch between " + inputFile.getAbsolutePath()
                    + " and " + gzipFile.getAbsolutePath() + " " + osha1 + " " + nsha1);
        }
        File ocdx = new File(subdir, inputFile.getName() + ".cdx");
        CDXOptions cdxOptions = new CDXOptions();
        cdxOptions.outputFile = ocdx;
        cdxOptions.filesList = new ArrayList<String>();
        cdxOptions.filesList.add(inputFile.getAbsolutePath());
        (new CDXTask()).runtask(cdxOptions);
        File ncdx = new File(subdir, gzipFile.getName() + ".cdx");
        cdxOptions.outputFile = ncdx;
        cdxOptions.filesList.clear();
        cdxOptions.filesList.add(gzipFile.getAbsolutePath());
        (new CDXTask()).runtask(cdxOptions);
        List<String> ocdxList;
        try {
            ocdxList = Files.readAllLines(ocdx.toPath());
        } catch (IOException e) {
            throw new FatalException(e);
        }
        List<String> ncdxList;
                try {
                    ncdxList = Files.readAllLines(ncdx.toPath());
                } catch (IOException e) {
                    throw new FatalException(e);
                }
        if ( ocdxList.size() != ncdxList.size()) {
            throw new FatalException("cdx files have different numbers of records.");
        }
        Iterator<String> ocdxIt =  ocdxList.iterator();
        Iterator<String> ncdxIt = ncdxList.iterator();
        while (ocdxIt.hasNext() && ncdxIt.hasNext()) {

        }
    }

    private File createAndGetOutputDir(File outputRootDir, String inputFileName) {
        String inputFilePrefix = inputFileName;
        int depth = Integer.parseInt(PreCompressor.propertiesMap.get("DEPTH"));
        while ( inputFilePrefix.length() < depth )  {
              inputFilePrefix = '0' + inputFilePrefix;
        }
        inputFilePrefix = inputFilePrefix.substring(0, depth);
        File subdir = outputRootDir;
        for (char subdirName: inputFilePrefix.toCharArray()) {
            subdir = new File(subdir, String.valueOf(subdirName));
            subdir.mkdirs();
        }
        return subdir;
    }

    public void run() {
          while (!sharedQueue.isEmpty()) {
              try {
                  String filename = sharedQueue.take();
                  precompress(filename);
              } catch (InterruptedException e) {
                  //What to do here?
              } catch (FatalException e) {
                  //Do some logging and stop the whole thing right here
                   System.exit(22);
              }
          }
    }
}
