package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import java.util.Iterator;
import java.util.logging.Level;

import org.apache.commons.io.output.NullOutputStream;
import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.jwat.arc.ArcDateParser;
import org.jwat.common.Uri;
import org.jwat.common.UriProfile;
import org.jwat.tools.tasks.cdx.CDXEntry;
import org.jwat.tools.tasks.cdx.CDXFile;
import org.jwat.tools.tasks.cdx.CDXFormatter;
import org.jwat.tools.tasks.cdx.CDXResult;
import org.jwat.tools.tasks.compress.CompressFile;
import org.jwat.tools.tasks.compress.CompressOptions;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import java.lang.StringBuilder;

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

    public void precompress(String filename) throws DeeplyTroublingException, WeirdFileException {
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
                throw new DeeplyTroublingException("Could not create ifile " + iFile.getAbsolutePath());
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

    private void checkConsistency(File inputFile, File gzipFile) throws DeeplyTroublingException {
        String nsha1;
        String osha1;
        try {
            osha1 = DigestUtils.sha1Hex(new FileInputStream(inputFile));
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
        try {
            nsha1 = DigestUtils.sha1Hex(new GZIPInputStream(new FileInputStream(gzipFile)));
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
        if (!nsha1.equals(osha1)) {
            final String message = "Checksum mismatch between " + inputFile.getAbsolutePath()
                    + " and " + gzipFile.getAbsolutePath() + " " + osha1 + " " + nsha1;
            deleteFile(gzipFile, false);
            throw new DeeplyTroublingException(message);
        }
    }

    private void deleteFile(File gzipFile, boolean writeMD5) throws DeeplyTroublingException {
        if (writeMD5) {
            writeMD5(gzipFile);
        }
        gzipFile.setWritable(true);
        try {
            Files.delete(gzipFile.toPath());
        } catch (IOException e) {
            throw new DeeplyTroublingException("Could not delete " + gzipFile.getAbsolutePath(), e);
        }
        if (gzipFile.exists()) {
            System.out.println("File " + gzipFile.getAbsolutePath() + " could not be deleted. Deleting on exit.");
            gzipFile.deleteOnExit();
        }
    }

    private static synchronized void writeMD5(File gzipFile) throws DeeplyTroublingException {
        String md5;
        try {
            md5 = DigestUtils.md5Hex(new FileInputStream(gzipFile));
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
        String md5Filepath = Util.getProperties().getProperty(Util.MD5_FILEPATH);
        writeToFile(new File(md5Filepath), gzipFile.getName() + "##" + md5, 5, 1000L);
    }

    private static synchronized void writeToFile(File file, String msg, int tries, long delay) throws DeeplyTroublingException {
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

    private static synchronized void writeCompressionLog(String message) throws DeeplyTroublingException {
        String compressionLogPath = Util.getProperties().getProperty(Util.LOG);
        (new File(compressionLogPath)).getParentFile().mkdirs();
        writeToFile(new File(compressionLogPath), message, 5, 1000L);
    }


    private void writeiFile(File uncompressedFile, File compressedFile, File iFile, File cdxFile) throws DeeplyTroublingException, WeirdFileException {
        CDXFormatter formatter = new CDXFormatter();
        CDXFile uncompressedCDXFile = new CDXFile();
        CDXResult uncompressedResult = uncompressedCDXFile.processFile(uncompressedFile);
        CDXFile compressedCDXFile = new CDXFile();
        CDXResult compressedResult = compressedCDXFile.processFile(compressedFile);
        Iterator<CDXEntry> ocdxIt = uncompressedResult.getEntries().iterator();
        Iterator<CDXEntry> ncdxIt = compressedResult.getEntries().iterator();
        String waybackCdxSpec = " CDX N b a m s k r V g";
        try (
                PrintWriter ifileWriter = new PrintWriter(new BufferedWriter(new FileWriter(iFile, true)));
                PrintWriter cdxWriter = new PrintWriter(new BufferedWriter(new FileWriter(cdxFile, true)))
        ) {
            cdxWriter.println(waybackCdxSpec);
            while (ocdxIt.hasNext() && ncdxIt.hasNext()) {
                CDXEntry oEntry = ocdxIt.next();
                CDXEntry nEntry = ncdxIt.next();
                ifileWriter.println(oEntry.offset + " " + nEntry.offset + " " + oEntry.date.getTime());
                cdxWriter.println(formatter.cdxEntry(nEntry,compressedFile.getName(),"NbamskrVg".toCharArray()));
            }
        } catch (IOException | NullPointerException e) {
            throw new WeirdFileException("Problem indexing files " + uncompressedFile.getAbsolutePath() + " " + compressedFile.getAbsolutePath(), e);
        } finally {

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
                } catch (DeeplyTroublingException e) {
                    // isDead = true;
                    errorMessage = e.getMessage();
                    throw new RuntimeException("Could not write log entry for " + filename, e);
                }
            } catch (DeeplyTroublingException | InterruptedException e) {
                //Mark as dead and let this thread die.
                //isDead = true;
                errorMessage = e.getMessage();
                try {
                    writeCompressionLog(filename + " not processed successfully " + errorMessage);
                } catch (DeeplyTroublingException e1) {
                    throw new RuntimeException("Could not write compression log for " + filename ,  e1);
                }
                //throw new RuntimeException(e);
            }
        }
    }

    public String cdxEntry(CDXEntry entry, String filename, char[] format) {
        StringBuilder sb = new StringBuilder();
        sb.setLength(0);
        char c;
        Uri uri;
        String host;
        int port;
        String query;
        for (int i = 0; i < format.length; ++i) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            c = format[i];
            switch (c) {
                case 'b':
                    if (entry.date != null) {
                        sb.append(ArcDateParser.getDateFormat().format(entry.date));
                    } else {
                        sb.append('-');
                    }
                    break;
                case 'e':
                    if (entry.ip != null && entry.ip.length() > 0) {
                        sb.append(entry.ip);
                    } else {
                        sb.append('-');
                    }
                    break;
                case 'A':
                case 'N':
                    if (entry.url != null && entry.url.length() > 0) {
                        uri = Uri.create(entry.url, UriProfile.RFC3986_ABS_16BIT_LAX);
                        StringBuilder cUrl = new StringBuilder();
                        if ("http".equalsIgnoreCase(uri.getScheme())) {
                            host = uri.getHost();
                            port = uri.getPort();
                            query = uri.getRawQuery();
                            if (host.startsWith("www.")) {
                                host = host.substring("www.".length());
                            }
                            cUrl.append(host);
                            if (port != -1 && port != 80) {
                                cUrl.append(':');
                                cUrl.append(port);
                            }
                            cUrl.append(uri.getRawPath());
                            if (query != null) {
                                cUrl.append('?');
                                cUrl.append(query);
                            }
                            sb.append(cUrl.toString().toLowerCase());
                        } else {
                            sb.append(entry.url.toLowerCase());
                        }
                    } else {
                        sb.append('-');
                    }
                    break;
                case 'a':
                    if (entry.url != null && entry.url.length() > 0) {
                        sb.append(entry.url);
                    } else {
                        sb.append('-');
                    }
                    break;
                case 'm':
                    if (entry.mimetype != null && entry.mimetype.length() > 0) {
                        sb.append(entry.mimetype);
                    } else {
                        sb.append('-');
                    }
                    break;
                case 's':
                    if (entry.responseCode != null && entry.responseCode.length() > 0) {
                        sb.append(entry.responseCode);
                    } else {
                        sb.append('-');
                    }
                    break;
                case 'c':
                    if (entry.checksum != null && entry.checksum.length() > 0) {
                        sb.append(entry.checksum);
                    } else {
                        sb.append('-');
                    }
                    break;
                case 'v':
                case 'V':
                    sb.append(entry.offset);
                    break;
                case 'n':
                    sb.append(entry.length);
                    break;
                case 'g':
                    sb.append(filename);
                    break;
                case '-':
                default:
                    sb.append('-');
                    break;
            }
        }
        return sb.toString();
    }

}

