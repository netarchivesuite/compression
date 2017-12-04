package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.jwat.arc.ArcDateParser;
import org.jwat.common.Uri;
import org.jwat.common.UriProfile;
import org.jwat.tools.tasks.cdx.CDXEntry;
import org.jwat.tools.tasks.cdx.CDXFile;
import org.jwat.tools.tasks.cdx.CDXFormatter;
import org.jwat.tools.tasks.cdx.CDXOptions;
import org.jwat.tools.tasks.cdx.CDXResult;
import org.jwat.tools.tasks.compress.CompressFile;
import org.jwat.tools.tasks.compress.CompressOptions;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.netarkivet.common.utils.cdx.CDXRecord;
import dk.netarkivet.common.utils.cdx.CDXUtils;

/**
 * Created by csr on 12/8/16.
 */
public class PrecompressionRunnable extends CompressFile implements Runnable {

    private static boolean isDead = false;
    private static Throwable error = null;

    private final BlockingQueue<String> sharedQueue;
    private int threadNo;

    private ArcIndexer arcIndexer = new ArcIndexer();
    private WarcIndexer warcIndexer = new WarcIndexer();
    private UrlCanonicalizer canonicalizer = new AggressiveUrlCanonicalizer();
    
    private final int defaultRecordHeaderMaxSize = 1024 * 1024;
    private final int defaultPayloadHeaderMaxSize = 1024 * 1024;

    public PrecompressionRunnable(BlockingQueue<String> sharedQueue, int threadNo) {
        this.sharedQueue = sharedQueue;
        this.threadNo = threadNo;
        arcIndexer.setCanonicalizer(canonicalizer);
        warcIndexer.setCanonicalizer(canonicalizer);
    }

    public void precompress(String filename) throws DeeplyTroublingException, WeirdFileException {
        File inputFile = new File(filename);
        if (inputFile.length() == 0) {
            writeCompressionLog(inputFile.getAbsolutePath() + " not compressed. Zero size file.", threadNo);
            return;
        }
        File iFileSubdir = Util.getIFileSubdir(inputFile.getName(), true);
        File cdxSubdir = Util.getCDXSubdir(inputFile.getName(), true);
        File iFile = new File(iFileSubdir, inputFile.getName() + ".ifile.cdx");
        if (iFile.exists()) {
            System.out.println("Thread # " + threadNo + ": File " + iFile.getAbsolutePath() + " already exists so skipping");
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
                writeCompressionLog("Writing empty ifile for compressed file " + gzipFile.getAbsolutePath(), threadNo);
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
        try (InputStream is = new FileInputStream(inputFile)) {
            osha1 = DigestUtils.sha1Hex(is);
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
        try (InputStream is = new GZIPInputStream(new FileInputStream(gzipFile))) {
            nsha1 = DigestUtils.sha1Hex(is);
        } catch (IOException e) {
            throw new DeeplyTroublingException(e);
        }
        if (!nsha1.equals(osha1)) {
            final String message = "Thread # " + threadNo + ": Checksum mismatch between " + inputFile.getAbsolutePath()
                    + " and " + gzipFile.getAbsolutePath() + " " + osha1 + " " + nsha1;
            deleteFile(gzipFile, false);
            throw new DeeplyTroublingException(message);
        }
    }

    /**
     * Delete the given file
     * @param gzipFile
     * @param writeMD5
     * @throws DeeplyTroublingException
     */
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
            System.out.println("Thread # " + threadNo + ": File " + gzipFile.getAbsolutePath() + " could not be deleted. Deleting on exit.");
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
        Util.writeToFile(new File(md5Filepath), gzipFile.getName() + "##" + md5, 5, 1000L);
    }
    
    private static synchronized void writeCompressionLog(String message, int threadNo) throws DeeplyTroublingException {
        String compressionLogPath = Util.getProperties().getProperty(Util.LOG);
        String dateprefix = "[" +  new Date() + " (thread: " + threadNo + ")] ";
        message = dateprefix + message;
        (new File(compressionLogPath)).getParentFile().mkdirs();
        Util.writeToFile(new File(compressionLogPath), message, 5, 1000L);
    }

    private void writeiFile(File uncompressedFile, File compressedFile, File iFile, File cdxFile) throws DeeplyTroublingException, WeirdFileException {
    	final int recordHeaderMaxSize = getIntProperty(Util.RECORD_HEADER_MAXSIZE, defaultRecordHeaderMaxSize);
        final int payloadHeaderMaxSize = getIntProperty(Util.PAYLOAD_HEADER_MAXSIZE, defaultPayloadHeaderMaxSize);
    	CDXFormatter formatter = new CDXFormatter();
        CDXOptions cdxOptions = new CDXOptions();
        cdxOptions.recordHeaderMaxSize = recordHeaderMaxSize;
        cdxOptions.payloadHeaderMaxSize = payloadHeaderMaxSize;
        List<CDXRecord> wacCdxRecords;
        // Uncompressed CDXRecords.
    	CDXFile uncompressedCDXFile = new CDXFile();
        CDXResult uncompressedResult = uncompressedCDXFile.processFile(uncompressedFile, cdxOptions);
        if (uncompressedResult.hasFailed()) {
        	throw new WeirdFileException("CDX generation of " + uncompressedFile.getAbsolutePath() + "failed!");
        }
        wacCdxRecords = getCDXRecordsUsingWAC(uncompressedFile);
        if (wacCdxRecords == null) {
        	throw new WeirdFileException("CDX validation of " + uncompressedFile.getAbsolutePath() + "failed!");
        }
        compareCdxRecords(wacCdxRecords, uncompressedResult.getEntries());
        // Compressed CDXRecords.
        CDXFile compressedCDXFile = new CDXFile();
        CDXResult compressedResult = compressedCDXFile.processFile(compressedFile, cdxOptions);
        if (compressedResult.hasFailed()) {
        	throw new WeirdFileException("CDX generation of " + compressedFile.getAbsolutePath() + "failed!");
        }
        wacCdxRecords = getCDXRecordsUsingWAC(compressedFile);
        if (wacCdxRecords == null) {
        	throw new WeirdFileException("CDX validation of " + compressedFile.getAbsolutePath() + "failed!");
        }
        compareCdxRecords(wacCdxRecords, compressedResult.getEntries());
        // Generate ifile and cdx file.
        List<CDXEntry> ocdx = uncompressedResult.getEntries();
        List<CDXEntry> ncdx = compressedResult.getEntries();
        CDXEntry oEntry;
        CDXEntry nEntry;
        String waybackCdxSpec = " CDX N b a m s k r V g";
        try (
                PrintWriter ifileWriter = new PrintWriter(new BufferedWriter(new FileWriter(iFile, true)));
                PrintWriter cdxWriter = new PrintWriter(new BufferedWriter(new FileWriter(cdxFile, true)))
        ) {
        	int idx = 0;
            cdxWriter.println(waybackCdxSpec);
            while (idx < ocdx.size() && idx < ncdx.size()) {
                oEntry = ocdx.get(idx);
                nEntry = ncdx.get(idx);
                ifileWriter.println(oEntry.offset + " " + nEntry.offset + " " + oEntry.date.getTime());
                cdxWriter.println(formatter.cdxEntry(nEntry,compressedFile.getName(),"NbamskrVg".toCharArray()));
                ++idx;
            }
            if (idx < ocdx.size()) {
            	throw new WeirdFileException(uncompressedFile.getAbsolutePath() + " has more entries than " + compressedFile.getAbsolutePath());
            }
            if (idx < ncdx.size()) {
            	throw new WeirdFileException(compressedFile.getAbsolutePath() + " has more entries than " + uncompressedFile.getAbsolutePath());
            }
        } catch (Throwable t) {
            throw new WeirdFileException("Problem indexing files " + uncompressedFile.getAbsolutePath() + " " + compressedFile.getAbsolutePath(), t);
        }
    }

    private int getIntProperty(String propertyKey, int defaultPropertyValue) {
        String propertyValue = Util.getProperties().getProperty(propertyKey);
        if (propertyValue == null) {
            return defaultPropertyValue;
        } else {
            int propertyValueInt = Integer.decode(propertyValue);
            return propertyValueInt;
        }
    }

    public void run() {
        while (!sharedQueue.isEmpty() && !isDead) {
            String filename = null;
            try {
                writeCompressionLog("Files left in the sharedQueue: " + sharedQueue.size(), threadNo);
                filename = sharedQueue.poll();
                if (filename != null) {
                    writeCompressionLog("Precompress of file " + filename + " started. Left in queue: " + sharedQueue.size(), threadNo);
                    precompress(filename);
                    writeCompressionLog(filename + " processed successfully.", threadNo);
                } else {
                    writeCompressionLog("Queue seems to be empty now. Nothing more to do.", threadNo);
                }
            } catch (WeirdFileException we) {
                try {
                    writeCompressionLog(filename + " could not be processed.", threadNo);
                } catch (DeeplyTroublingException e) {
                    // isDead = true;
                    error = e;
                    throw new RuntimeException("Could not write log entry for " + filename, e);
                }
            } catch (DeeplyTroublingException e) {
                //Mark as dead and let this thread die.
                //isDead = true;
                error = e;
                try {
                    writeCompressionLog(filename + " not processed successfully. Cause: " + error, threadNo);
                } catch (DeeplyTroublingException e1) {
                    throw new RuntimeException("Could not write compression log for " + filename ,  e1);
                }
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

    public List<CDXRecord> getCDXRecordsUsingWAC(File inputFile) {
        ByteArrayOutputStream bOut = null;
        ByteArrayInputStream bIn = null;
        BufferedReader reader = null;
        List<CDXRecord> entries = new ArrayList<CDXRecord>();
        try {
            bOut = new ByteArrayOutputStream();
            CDXUtils.writeCDXInfo(inputFile, bOut);
            bIn = new ByteArrayInputStream(bOut.toByteArray());
            reader = new BufferedReader(new InputStreamReader(bIn, "UTF-8"));
            String tmpStr;
            while ((tmpStr = reader.readLine()) != null) {
                String[] fieldParts = tmpStr.split("\\s+");
                CDXRecord cdxrec = null;
                try {
                    cdxrec = new CDXRecord(fieldParts);
                    entries.add(cdxrec);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        } catch (Throwable t) {
        	entries = null;
        	t.printStackTrace();
        } finally {
            if (bOut != null) {
                bOut.reset();
            }
            IOUtils.closeQuietly(bOut);
            IOUtils.closeQuietly(bIn);
            IOUtils.closeQuietly(reader);
        }
        return entries;
    }

    public boolean compareCdxRecords(List<CDXRecord> wacCdxRecords, List<CDXEntry> jwatCdxEntries) throws WeirdFileException {
    	CDXRecord wacCdxRecord;
    	CDXEntry jwatCdxEntry;
    	int idx = 0;
    	while (idx < wacCdxRecords.size() && idx < jwatCdxEntries.size()) {
    		wacCdxRecord = wacCdxRecords.get(idx);
    		jwatCdxEntry = jwatCdxEntries.get(idx);
    		if (wacCdxRecord.getOffset() != jwatCdxEntry.offset) {
        		throw new WeirdFileException("Offsets differ: " + wacCdxRecord.getOffset() + " - " + jwatCdxEntry.offset);
    		}
    		/*
    		if (wacCdxRecord.getLength() != jwatCdxEntry.length) {
        		throw new WeirdFileException("Lengths differ: " + wacCdxRecord.getLength() + " - " + jwatCdxEntry.length);
    		}
    		*/
    		if (wacCdxRecord.getURL().compareToIgnoreCase(jwatCdxEntry.url) != 0) {
        		throw new WeirdFileException("Urls differ: " + wacCdxRecord.getURL() + " - " + jwatCdxEntry.url);
    		}
    		/*
    		if (wacCdxRecord.getMimetype().compareToIgnoreCase(jwatCdxEntry.mimetype) != 0) {
        		throw new WeirdFileException("Mimwtypes differ: " + wacCdxRecord.getMimetype() + " - " + jwatCdxEntry.mimetype);
    		}
    		*/
    		if (wacCdxRecord.getDate().compareToIgnoreCase(ArcDateParser.getDateFormat().format(jwatCdxEntry.date)) != 0) {
        		throw new WeirdFileException("Dates differ: " + wacCdxRecord.getDate() + " - " + jwatCdxEntry.date);
    		}
    		++idx;
    	}
    	if (idx < wacCdxRecords.size()) {
    		throw new WeirdFileException("Number of records not identical: nascdx=" + wacCdxRecords.size() + " offset=" + wacCdxRecords.get(idx).getOffset() + " > jwatcdx=" + jwatCdxEntries.size());
    	}
    	if (idx < jwatCdxEntries.size()) {
    		throw new WeirdFileException("Number of records not identical: jwatcdx=" + jwatCdxEntries.size() + " offset=" + jwatCdxEntries.get(idx).offset + " > nascdx=" + wacCdxRecords.size());
    	}
    	return false;
    }

    private static class DEBUG {
    }

}
