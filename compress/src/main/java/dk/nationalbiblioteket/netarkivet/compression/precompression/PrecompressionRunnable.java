package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;

import dk.nationalbiblioteket.netarkivet.compression.FatalException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import java.util.logging.Level;

import org.apache.commons.io.output.NullOutputStream;
import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.SearchResultToCDXFormatAdapter;
import org.archive.wayback.resourceindex.cdx.format.CDXFormat;
import org.archive.wayback.resourceindex.cdx.format.CDXFormatException;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
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
public class PrecompressionRunnable extends CompressFile implements Runnable {


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
    private UrlCanonicalizer canonicalizer = new AggressiveUrlCanonicalizer();

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

    public PrecompressionRunnable(BlockingQueue<String> sharedQueue, int threadNo) {
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
        arcIndexer.setCanonicalizer(canonicalizer);
        warcIndexer.setCanonicalizer(canonicalizer);
    }

    public void precompress(String filename) throws FatalException, WeirdFileException {
        File inputFile = new File(filename);
        if (inputFile.length() == 0) {
            writeCompressionLog(inputFile.getAbsolutePath() + " not compressed. Zero size file.");
            return;
        }
        File iFileSubdir = Util.getIFileSubdir(inputFile.getName(), true);
        File cdxSubdir = Util.getCDXSubdir(inputFile.getName(), true);
        File iFile = new File(iFileSubdir, inputFile.getName() + ".ifile.cdx");
        if (iFile.exists()) {
            System.out.println("File " + iFile.getAbsolutePath() + " already exists so not reprocessing.");
            return;
        }
        File gzipFile = doCompression(inputFile);
        checkConsistency(inputFile, gzipFile);
        File cdxFile = new File(cdxSubdir, gzipFile.getName() + ".cdx");
        if (!gzipFile.getName().contains("metadata")) {
            writeiFile(inputFile, gzipFile, iFile, cdxFile);
        } else {
            try {
                //Create an empty file here as a placeholder to show that this input file has been processed.
                iFile.createNewFile();
            } catch (IOException e) {
                throw new FatalException("Could not create ifile " + iFile.getAbsolutePath());
            }
        }
        deleteFile(gzipFile, true);
    }

    private File doCompression(File inputFile) throws WeirdFileException {
        File tmpdir = (new File(Util.getProperties().getProperty(Util.TEMP_DIR)));
        tmpdir.mkdirs();
        CompressOptions compressOptions = new CompressOptions();
        compressOptions.dstPath = tmpdir;
        compressOptions.bTwopass = true;
        compressOptions.compressionLevel = Integer.parseInt(Util.COMPRESSION_LEVEL);
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
                    IOUtils.copy(p.getInputStream(), new NullOutputStream());
                    IOUtils.copy(p.getErrorStream(), new NullOutputStream());
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
        String md5Filepath = Util.getProperties().getProperty(Util.MD5_FILEPATH);
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(md5Filepath, true)))) {
            writer.println(gzipFile.getName() + "##" + md5);
        } catch (IOException e) {
            throw new FatalException(e);
        }
    }

    private static synchronized void writeCompressionLog(String message) throws FatalException {
        String compressionLogPath = Util.getProperties().getProperty(Util.LOG);
        (new File(compressionLogPath)).getParentFile().mkdirs();
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(compressionLogPath, true)))) {
            writer.println(message);
        } catch (IOException e) {
            throw new FatalException(e);
        }
    }


    private void writeiFile(File uncompressedFile, File compressedFile, File iFile, File cdxFile) throws FatalException, WeirdFileException {
        CloseableIterator<CaptureSearchResult> ocdxIt;
        CloseableIterator<CaptureSearchResult> ncdxIt;
        String cdxSpec = " CDX A b a m s k r V g";
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
                PrintWriter ifileWriter = new PrintWriter(new BufferedWriter(new FileWriter(iFile, true)));
                PrintWriter cdxWriter = new PrintWriter(new BufferedWriter(new FileWriter(cdxFile, true)))
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
