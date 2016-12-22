package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;

import dk.nationalbiblioteket.netarkivet.compression.Util;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import java.util.logging.Level;
import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.SearchResultToCDXFormatAdapter;
import org.archive.wayback.resourceindex.cdx.format.CDXFormat;
import org.archive.wayback.resourceindex.cdx.format.CDXFormatException;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;
import org.archive.wayback.util.url.IdentityUrlCanonicalizer;
import org.jwat.tools.tasks.compress.CompressFile;
import org.jwat.tools.tasks.compress.CompressOptions;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import static java.lang.Thread.sleep;

/**
 * Created by csr on 12/8/16.
 */
public class Consumer  extends CompressFile implements Runnable {


    static {
        Logger logger = Logger.getLogger("org.archive.wayback.resourcestore.indexer");
        logger.setLevel(Level.WARNING);
    }


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

    public void precompress(String filename) throws FatalException, WeirdFileException {
        String outputRootDirName = PreCompressor.properties.getProperty(PreCompressor.OUTPUT_ROOT_DIR);
        File inputFile = new File(filename);
        if (inputFile.length() == 0) {
            writeCompressionLog(inputFile.getAbsolutePath() + " not compressed. Zero size file.");
            return;
        }
        File subdir = Util.getIFileSubdir(inputFile.getName(), true);
        File outputFile = new File(subdir, inputFile.getName() + ".ifile.cdx");
        if (outputFile.exists()) {
            //already done this one
            return;
        }
        File gzipFile = doCompression(inputFile);
        checkConsistency(inputFile, gzipFile);
        if (!gzipFile.getName().contains("metadata")) {
            writeiFile(inputFile, gzipFile, outputFile);
        } else {
            try {
                //Create an empty file here as a placeholder to show that this input file has been processed.
                outputFile.createNewFile();
            } catch (IOException e) {
                throw new FatalException("Could not create ifile " + outputFile.getAbsolutePath());
            }
        }
        deleteFile(gzipFile, true);
    }

    private File doCompression(File inputFile) throws WeirdFileException {
        File tmpdir = (new File(PreCompressor.properties.getProperty(PreCompressor.TEMP_DIR)));
        tmpdir.mkdirs();
        CompressOptions compressOptions = new CompressOptions();
        compressOptions.dstPath = tmpdir;
        compressOptions.bTwopass = true;
        compressOptions.compressionLevel = 9;
        this.compressFile(inputFile, compressOptions);
        File gzipFile = new File (tmpdir, inputFile.getName() + ".gz");
        if (!gzipFile.exists()) {
            throw new WeirdFileException("Compressed file " + gzipFile.getAbsolutePath() + " not created.");
        } else {
            return gzipFile;
        }
    }

    private void checkConsistency(File inputFile, File gzipFile) throws FatalException {
        String nsha1;
        String osha1;
        try {
            osha1 = DigestUtils.sha1Hex(new FileInputStream(inputFile));
        } catch (IOException e) {
            throw new FatalException(e);
        }
        try {
            nsha1 = DigestUtils.sha1Hex(new GZIPInputStream(new FileInputStream(gzipFile)));
        } catch (IOException e) {
            throw new FatalException(e);
        }
        if (!nsha1.equals(osha1)) {
            final String message = "Checksum mismatch between " + inputFile.getAbsolutePath()
                    + " and " + gzipFile.getAbsolutePath() + " " + osha1 + " " + nsha1;
            deleteFile(gzipFile, false);
            throw new FatalException(message);
        }
    }

    private void deleteFile(File gzipFile, boolean writeMD5) throws FatalException {
        if (writeMD5) {
            writeMD5(gzipFile);
        }
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


    private void writeiFile(File uncompressedFile, File compressedFile, File outputFile) throws FatalException, WeirdFileException {
        CloseableIterator<CaptureSearchResult> ocdxIt;
        CloseableIterator<CaptureSearchResult> ncdxIt;
        File ncdxFile = new File(outputFile.getParentFile(), compressedFile.getName() + ".cdx");
        String cdxSpec = " CDX N b a m s k r M V g";
        SearchResultToCDXFormatAdapter adapter;
        try {
            adapter = new SearchResultToCDXFormatAdapter(new CDXFormat(cdxSpec));
        } catch (CDXFormatException e) {
            throw new FatalException(e);
        }
        try {
            ocdxIt = indexFile(uncompressedFile.getAbsolutePath());
        } catch (IOException e) {
            throw new WeirdFileException("Problem reading " + uncompressedFile.getAbsolutePath(), e);
        }
        try {
            ncdxIt = indexFile(compressedFile.getAbsolutePath());
        } catch (IOException e) {
            throw new WeirdFileException("Problem reading " + compressedFile.getAbsolutePath(), e);
        }
        try (
                PrintWriter ifileWriter = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)));
                PrintWriter cdxWriter = new PrintWriter(new BufferedWriter(new FileWriter(ncdxFile, true)))
        ) {
            cdxWriter.println(cdxSpec);
            while (ocdxIt.hasNext() && ncdxIt.hasNext()) {
                CaptureSearchResult oResult = ocdxIt.next();
                CaptureSearchResult nResult = ncdxIt.next();
                ifileWriter.println(oResult.getOffset() + " " + nResult.getOffset() + " " + oResult.getCaptureTimestamp());
                cdxWriter.println(adapter.adapt(nResult));
            }
        } catch (IOException e) {
            throw new WeirdFileException("Problem indexing files " + uncompressedFile.getAbsolutePath() + " " + compressedFile.getAbsolutePath(), e);
        } finally {
            try {
                ocdxIt.close();
                ncdxIt.close();
            } catch (IOException e) {
                throw new FatalException(e);
            }
        }
    }

    public void run() {
        while (!sharedQueue.isEmpty() && !isDead) {
            String filename = null;
            try {
                filename = sharedQueue.take();
                precompress(filename);
                writeCompressionLog(filename + " processed successfully.");
            } catch (WeirdFileException we) {
                try {
                    writeCompressionLog(filename + " could not be processed.");
                } catch (FatalException e) {
                    isDead = true;
                    errorMessage = e.getMessage();
                    throw new RuntimeException("Could not write log entry", e);
                }
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
