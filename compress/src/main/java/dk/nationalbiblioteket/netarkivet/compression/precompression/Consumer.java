package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.io.IOUtils;
import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;
import org.archive.wayback.util.url.IdentityUrlCanonicalizer;
import org.jwat.tools.tasks.compress.CompressFile;
import org.jwat.tools.tasks.compress.CompressOptions;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;

import static java.lang.Thread.sleep;

/**
 * Created by csr on 12/8/16.
 */
public class Consumer  extends CompressFile implements Runnable {

    private static boolean isDead = false;
    private static String errorMessage = null;

    private final BlockingQueue<String> sharedQueue;
    private int threadNo;

    public static final String ARC_EXTENSION = ".arc";
    public static final String ARC_GZ_EXTENSION = ".arc.gz";
    public static final String WARC_EXTENSION = ".warc";
    public static final String WARC_GZ_EXTENSION = ".warc.gz";
    private ArcIndexer arcIndexer = new ArcIndexer();
    private WarcIndexer warcIndexer = new WarcIndexer();
    private UrlCanonicalizer canonicalizer = new IdentityUrlCanonicalizer();

    private CloseableIterator<CaptureSearchResult> indexFile(String pathOrUrl) throws IOException {
        CloseableIterator itr = null;
        if(pathOrUrl.endsWith(ARC_EXTENSION)) {
            itr = this.arcIndexer.iterator(pathOrUrl);
        } else if(pathOrUrl.endsWith(ARC_GZ_EXTENSION)) {
            itr = this.arcIndexer.iterator(pathOrUrl);
        } else if(pathOrUrl.endsWith(WARC_EXTENSION)) {
            itr = this.warcIndexer.iterator(pathOrUrl);
        } else if(pathOrUrl.endsWith(WARC_GZ_EXTENSION)) {
            itr = this.warcIndexer.iterator(pathOrUrl);
        }
        return itr;
    }

    public Consumer (BlockingQueue<String> sharedQueue, int threadNo) {
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
        arcIndexer.setCanonicalizer(canonicalizer);
        warcIndexer.setCanonicalizer(canonicalizer);
    }

    public void precompress(String filename) throws FatalException {
        String outputRootDirName = PreCompressor.properties.getProperty(PreCompressor.OUTPUT_ROOT_DIR);
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
        compressOptions.bTwopass = true;
        this.compressFile(inputFile, compressOptions);
        File gzipFile = new File (subdir, inputFile.getName() + ".gz");
        String nsha1;
        try {
            nsha1 = DigestUtils.sha1Hex(new GZIPInputStream(new FileInputStream(gzipFile)));
        } catch (IOException e) {
            throw new FatalException(e);
        }
        if (!nsha1.equals(osha1)) {
            final String message = "Checksum mismatch between " + inputFile.getAbsolutePath()
                    + " and " + gzipFile.getAbsolutePath() + " " + osha1 + " " + nsha1;
            deleteFile(gzipFile);
            throw new FatalException(message);
        }
        writeLookupFile(inputFile, gzipFile, outputFile);
        deleteFile(gzipFile);
    }

    private void deleteFile(File gzipFile) throws FatalException {
        writeMD5(gzipFile);
        gzipFile.setWritable(true);
        System.gc();
        if (System.getProperty("os.name").contains("Windows")){
            int tries = 0;
            int maxTries = 20;
            while (gzipFile.exists() && tries < maxTries) {
                try {
                    Process p = Runtime.getRuntime().exec("cmd /C del /F /Q " + gzipFile.getAbsolutePath());
                    IOUtils.copy(p.getInputStream(), System.out);
                    IOUtils.copy(p.getErrorStream(), System.err);
                    tries ++;
                    if (gzipFile.exists()) {
                        try {
                            sleep(5000);
                        } catch (InterruptedException e) {
                            gzipFile.deleteOnExit();
                            //throw new FatalException(e);
                        }
                    }
                } catch (IOException e) {
                    gzipFile.deleteOnExit();
                    //throw new FatalException(e);
                }
            }
            if (gzipFile.exists()) {
                gzipFile.deleteOnExit();
                //throw new FatalException("Could not delete " + gzipFile.getAbsolutePath());
            }
        } else {
            try {
                Files.delete(gzipFile.toPath());
            } catch (IOException e) {
                throw new FatalException("Could not delete " + gzipFile.getAbsolutePath(), e);
            }
        }
    }

    private static synchronized void writeMD5(File gzipFile) throws FatalException {
        String md5;
        try {
            md5 = DigestUtils.md5Hex(new FileInputStream(gzipFile));
        } catch (IOException e) {
            throw new FatalException(e);
        }
        String md5Filepath = PreCompressor.properties.getProperty(PreCompressor.MD5_FILEPATH);
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(md5Filepath, true)))) {
            writer.println(gzipFile.getName() + "##" + md5);
        } catch (IOException e) {
            throw new FatalException(e);
        }
    }

    private static synchronized void writeCompressionLog(String message) throws FatalException {
        String compressionLogPath = PreCompressor.properties.getProperty(PreCompressor.LOG);
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(compressionLogPath, true)))) {
            writer.println(message);
        } catch (IOException e) {
            throw new FatalException(e);
        }
    }


    private void writeLookupFile(File uncompressedFile, File compressedFile, File outputFile) throws FatalException {
        CloseableIterator<CaptureSearchResult> ocdxIt;
        CloseableIterator<CaptureSearchResult> ncdxIt;
        try {
            ocdxIt = indexFile(uncompressedFile.getAbsolutePath());
        } catch (IOException e) {
            throw new FatalException(e);
        }
        try {
            ncdxIt = indexFile(compressedFile.getAbsolutePath());
        } catch (IOException e) {
            throw new FatalException(e);
        }
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)))) {
            while (ocdxIt.hasNext() && ncdxIt.hasNext()) {
                CaptureSearchResult oResult = ocdxIt.next();
                CaptureSearchResult nResult = ncdxIt.next();
                writer.println(oResult.getOffset() + " " + nResult.getOffset() + " " + oResult.getCaptureTimestamp());
            }
        } catch (IOException e) {
            throw new FatalException(e);
        } finally {
            try {
                ocdxIt.close();
                ncdxIt.close();
            } catch (IOException e) {
                throw new FatalException(e);
            }
        }
    }

    private File createAndGetOutputDir(File outputRootDir, String inputFileName) {
        String inputFilePrefix = inputFileName;
        int depth = Integer.parseInt(PreCompressor.properties.getProperty(PreCompressor.DEPTH));
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
        while (!sharedQueue.isEmpty() && !isDead) {
            String filename = null;
            try {
                filename = sharedQueue.take();
                precompress(filename);
                writeCompressionLog(filename + " processed successfully.");
            } catch (Exception e) {
                //Mark as dead and let this thread die.
                isDead = true;
                errorMessage = e.getMessage();
                try {
                    writeCompressionLog(filename + " not processed successfully");
                } catch (FatalException e1) {
                    throw new RuntimeException(e1);
                }
                throw new RuntimeException(e);
            }
        }
    }
}
