package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.format.CDXFormat;
import org.archive.wayback.resourceindex.cdx.format.CDXFormatException;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.IndexWorker;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;
import org.archive.wayback.util.url.IdentityUrlCanonicalizer;
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
        CloseableIterator<CaptureSearchResult> ocdxIt;
        CloseableIterator<CaptureSearchResult> ncdxIt;
        try {
            ocdxIt = indexFile(inputFile.getAbsolutePath());
        } catch (IOException e) {
            throw new FatalException(e);
        }
        try {
            ncdxIt = indexFile(gzipFile.getAbsolutePath());
        } catch (IOException e) {
            throw new FatalException(e);
        }
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath())) {
            while (ocdxIt.hasNext() && ncdxIt.hasNext()) {
                CaptureSearchResult oResult = ocdxIt.next();
                CaptureSearchResult nResult = ncdxIt.next();
                writer.write(oResult.getOffset() + " " + nResult.getOffset() + " " + oResult.getCaptureTimestamp());
                writer.newLine();
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
