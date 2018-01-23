package dk.nationalbiblioteket.netarkivet.compression.precompression;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.Date;
import java.util.Iterator;
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
import org.jwat.arc.ArcHeader;
import org.jwat.arc.ArcReader;
import org.jwat.archive.ArchiveRecordParserCallback;
import org.jwat.common.Uri;
import org.jwat.common.UriProfile;
import org.jwat.tools.tasks.ResultItemThrowable;
import org.jwat.tools.tasks.cdx.CDXEntry;
import org.jwat.tools.tasks.cdx.CDXFile;
import org.jwat.tools.tasks.cdx.CDXFormatter;
import org.jwat.tools.tasks.cdx.CDXOptions;
import org.jwat.tools.tasks.cdx.CDXResult;
import org.jwat.tools.tasks.compress.CompressFile;
import org.jwat.tools.tasks.compress.CompressOptions;
import org.jwat.tools.tasks.compress.CompressResult;
import org.jwat.warc.WarcHeader;
import org.jwat.warc.WarcReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.nationalbiblioteket.netarkivet.compression.DeeplyTroublingException;
import dk.nationalbiblioteket.netarkivet.compression.Util;
import dk.nationalbiblioteket.netarkivet.compression.WeirdFileException;
import dk.nationalbiblioteket.netarkivet.compression.precompression.CDXRecordExtractorOutput.CDXRecord;

/**
 * Created by csr on 12/8/16.
 */
public class PrecompressionRunnable extends CompressFile implements Runnable {

    Logger logger = LoggerFactory.getLogger(PrecompressionRunnable.class);

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
    	final int recordHeaderMaxSize = getIntProperty(Util.RECORD_HEADER_MAXSIZE, defaultRecordHeaderMaxSize);
        final int payloadHeaderMaxSize = getIntProperty(Util.PAYLOAD_HEADER_MAXSIZE, defaultPayloadHeaderMaxSize);
        File tmpdir = (new File(Util.getProperties().getProperty(Util.TEMP_DIR)));
        tmpdir.mkdirs();
        CompressOptions compressOptions = new CompressOptions();
        compressOptions.dstPath = tmpdir;
        compressOptions.bTwopass = true;
        compressOptions.compressionLevel = Integer.parseInt(Util.COMPRESSION_LEVEL);
        compressOptions.recordHeaderMaxSize = recordHeaderMaxSize;
        compressOptions.payloadHeaderMaxSize = payloadHeaderMaxSize;
        if ("23312-55-20071125023519-00403-kb-prod-har-002.kb.dk.arc".equalsIgnoreCase(inputFile.getName())) {
            compressOptions.arpCallback = new ArchiveRecordParserCallback() {
    			@Override
    			public void arcParsedRecordHeader(ArcReader reader, long startOffset, ArcHeader header) {
    				if (startOffset == 81984113 && header.archiveLength == 14493) {
    					System.out.println(Long.toHexString(startOffset) + " " + startOffset + " " + header.archiveLength + " " + header.archiveLengthStr);
    					header.archiveLength = 8192L;
    					header.archiveLengthStr = "8192";
    				}
    			}
    			@Override
    			public void warcParsedRecordHeader(WarcReader reader, long startOffset, WarcHeader header) {
    			}
    		};
        }
        CompressResult result = this.compressFile(inputFile, compressOptions);
        File gzipFile = new File (tmpdir, inputFile.getName() + ".gz");
        if (!gzipFile.exists()) {
            throw new WeirdFileException("Compressed file " + gzipFile.getAbsolutePath() + " was not created.", result.getThrowable());
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
        if ("23312-55-20071125023519-00403-kb-prod-har-002.kb.dk.arc".equalsIgnoreCase(uncompressedFile.getName())) {
            cdxOptions.arpCallback = new ArchiveRecordParserCallback() {
    			@Override
    			public void arcParsedRecordHeader(ArcReader reader, long startOffset, ArcHeader header) {
    				if (startOffset == 81984113 && header.archiveLength == 14493) {
    					System.out.println(Long.toHexString(startOffset) + " " + startOffset + " " + header.archiveLength + " " + header.archiveLengthStr);
    					header.archiveLength = 8192L;
    					header.archiveLengthStr = "8192";
    				}
    			}
    			@Override
    			public void warcParsedRecordHeader(WarcReader reader, long startOffset, WarcHeader header) {
    			}
    		};
        }
        List<CDXRecord> wacCdxRecords;
        // Uncompressed CDXRecords.
    	CDXFile uncompressedCDXFile = new CDXFile();
        CDXResult uncompressedResult = uncompressedCDXFile.processFile(uncompressedFile, cdxOptions);
        if (uncompressedResult.hasFailed()) {
        	List<ResultItemThrowable> throwables = uncompressedResult.getThrowables();
        	if (throwables != null) {
        		Iterator<ResultItemThrowable> iter = throwables.iterator();
        		while (iter.hasNext()) {
        			iter.next().t.printStackTrace(System.err);
        		}
        	}
        	throw new WeirdFileException("CDX generation of " + uncompressedFile.getAbsolutePath() + " failed!");
        }
        wacCdxRecords = CDXRecordExtractorOutput.getCDXRecords(uncompressedFile);
        if (wacCdxRecords == null) {
        	throw new WeirdFileException("CDX validation of " + uncompressedFile.getAbsolutePath() + " failed!");
        }
        compareCdxRecords(wacCdxRecords, uncompressedResult.getEntries(), false, uncompressedFile.length());
        wacCdxRecords.clear();
        cdxOptions.arpCallback = null;
        // Compressed CDXRecords.
        CDXFile compressedCDXFile = new CDXFile();
        CDXResult compressedResult = compressedCDXFile.processFile(compressedFile, cdxOptions);
        if (compressedResult.hasFailed()) {
        	throw new WeirdFileException("CDX generation of " + compressedFile.getAbsolutePath() + " failed!");
        }
        wacCdxRecords = CDXRecordExtractorOutput.getCDXRecords(compressedFile);
        if (wacCdxRecords == null) {
        	throw new WeirdFileException("CDX validation of " + compressedFile.getAbsolutePath() + " failed!");
        }
        compareCdxRecords(wacCdxRecords, compressedResult.getEntries(), true, compressedFile.length());
        wacCdxRecords.clear();
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
                if (oEntry.date != null) {
                    ifileWriter.println(oEntry.offset + " " + nEntry.offset + " " + oEntry.date.getTime());
                } else {
                    ifileWriter.println(oEntry.offset + " " + nEntry.offset + " " + -1);
                }
                cdxWriter.println(formatter.cdxEntry(nEntry, compressedFile.getName(), "NbamskrVg".toCharArray()));
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
                    logger.warn(filename + " could not be processed.", we);
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

    public static final int S_BALANCE = 0;
    public static final int S_DISTURBANCE = 1;

    public boolean compareCdxRecords(List<CDXRecord> wacCdxRecords, List<CDXEntry> jwatCdxEntries, boolean bCompressed, long filelength) throws WeirdFileException {
    	CDXRecord wacCdxRecord;
    	CDXEntry jwatCdxEntry;
    	int wIdx = 0;
    	int jIdx = 0;
    	int state = S_BALANCE;
    	boolean bEqual;
    	String wacMimetype;
    	String jwatMimetype;
    	int disturbances = 0;
    	int disturbancesInARow = 0;
    	int nulls = 0;
    	while (wIdx < wacCdxRecords.size() && jIdx < jwatCdxEntries.size()) {
    		if (wIdx < wacCdxRecords.size()) {
        		wacCdxRecord = wacCdxRecords.get(wIdx);
    		} else {
        		wacCdxRecord = null;
    		}
    		if (jIdx < jwatCdxEntries.size()) {
        		jwatCdxEntry = jwatCdxEntries.get(jIdx);
        		/*
        		System.out.println(wacCdxRecord.offset);
        		System.out.println(jwatCdxEntry.offset);
        		System.out.println(wacCdxRecord.gzLen);
        		System.out.println(jwatCdxEntry.length);
    			System.out.println(wacCdxRecord.origUrl);
    			System.out.println(jwatCdxEntry.url);
    			System.out.println(wacCdxRecord.mime);
    			System.out.println(jwatCdxEntry.mimetype);
    			System.out.println(wacCdxRecord.date);
    			System.out.println(ArcDateParser.getDateFormat().format(jwatCdxEntry.date));
    			*/
    		} else {
        		jwatCdxEntry = null;
    		}
    		switch (state) {
    		case S_BALANCE:
    			bEqual = true;
    			if (wacCdxRecord != null && jwatCdxEntry != null) {
    				if (!"-".equals(wacCdxRecord.offset)) {
    					// Offsets differ.
    		    		if (Long.parseLong(wacCdxRecord.offset) != jwatCdxEntry.offset) {
    		    			logger.warn("(" + wIdx + ":" + jIdx + ") Offsets differ: " + wacCdxRecord.offset + " - " + jwatCdxEntry.offset);
    		    			/*
    		    			 * Check if the rest of the data matches.
    		    			 */
    	    			    if (wacCdxRecord.origUrl.compareToIgnoreCase(jwatCdxEntry.url) != 0) {
    	    			    	bEqual = false;
    	    	        		logger.warn("(" + wIdx + ":" + jIdx + ") Urls differ: " + wacCdxRecord.origUrl + " - " + jwatCdxEntry.url);
    	    	    		}
	    	            	wacMimetype = cleanupMimetype(wacCdxRecord.mime);
	    	            	jwatMimetype = cleanupMimetype(jwatCdxEntry.mimetype);
    	    	            if (wacMimetype.compareToIgnoreCase(jwatMimetype) != 0) {
    	    	            	if (!"no-type".equalsIgnoreCase(wacMimetype) || !"application/http".equalsIgnoreCase(jwatMimetype)) {
        	    			    	bEqual = false;
    	    	            		logger.warn("(" + wIdx + ":" + jIdx + ") Mimetypes differ: " + wacMimetype + " - " + jwatMimetype);
    	    	            	}
    	    	            }
    	    	            if (wacCdxRecord.date.compareToIgnoreCase(ArcDateParser.getDateFormat().format(jwatCdxEntry.date)) != 0) {
    	    			    	bEqual = false;
    	    	            	logger.warn("(" + wIdx + ":" + jIdx + ") Dates differ: " + wacCdxRecord.date + " - " + jwatCdxEntry.date);
    	    	            }
    	    	            ++disturbances;
    	    	            ++disturbancesInARow;
    	    	            if (!bEqual) {
    	    	            	if (Long.parseLong(wacCdxRecord.offset) < jwatCdxEntry.offset) {
    	    	            		++wIdx;
    	    	            	} else {
    	    	            		++jIdx;
    	    	            	}
    	    	            } else {
	    	            		++wIdx;
	    	            		++jIdx;
    	    	            }
    				    } else {
    	    			    if (wacCdxRecord.origUrl.compareToIgnoreCase(jwatCdxEntry.url) != 0) {
    	    			    	bEqual = false;
    	    	        		logger.warn("(" + wIdx + ":" + jIdx + ") Urls differ: " + wacCdxRecord.origUrl + " - " + jwatCdxEntry.url);
    	    	    		}
	    	            	wacMimetype = cleanupMimetype(wacCdxRecord.mime);
	    	            	jwatMimetype = cleanupMimetype(jwatCdxEntry.mimetype);
    	    	            if (wacMimetype.compareToIgnoreCase(jwatMimetype) != 0) {
    	    	            	if (!"no-type".equalsIgnoreCase(wacMimetype) || !"application/http".equalsIgnoreCase(jwatMimetype)) {
        	    			    	bEqual = false;
    	    	            		logger.warn("(" + wIdx + ":" + jIdx + ") Mimetypes differ: " + wacMimetype + " - " + jwatMimetype);
    	    	            	}
    	    	            }
    	    	            if (wacCdxRecord.date.compareToIgnoreCase(ArcDateParser.getDateFormat().format(jwatCdxEntry.date)) != 0) {
    	    			    	bEqual = false;
    	    	            	logger.warn("(" + wIdx + ":" + jIdx + ") Dates differ: " + wacCdxRecord.date + " - " + jwatCdxEntry.date);
    	    	            }
    	    	            if (bEqual) {
        	    	            disturbancesInARow = 0;
    	    	            } else {
        	    	            ++disturbances;
        	    	            ++disturbancesInARow;
    	    	            }
    	    	            ++wIdx;
    	    	            ++jIdx;
    				    }
    				} else {
	    	            ++disturbances;
	    	            ++disturbancesInARow;
	            		++wIdx;
    				}
    		        if (disturbancesInARow > 2) {
    		        	throw new WeirdFileException("3 disturbances in a row!");
    		        }
    			} else if (wacCdxRecord == null) {
    				// WAC fucked up again...
    	            ++wIdx;
    	            ++nulls;
    	            state = S_DISTURBANCE;
    			} else {
	                throw new WeirdFileException("JWAT CDX entry is null!");
    			}
    			break;
    		case S_DISTURBANCE:
    			if (wacCdxRecord == null) {
    				// WAC fucked up again...
    	            ++wIdx;
    	            ++nulls;
    			} else {
        			state = S_BALANCE;
    			}
    			break;
    		default:
    			throw new WeirdFileException("Luke I AM your father!");
    		}
			/*
			// TODO length in jwat is the record content and gzlen in wac is the complete compressed gzip entry it seems.
			// Rather undocumented though.
    		if (!"-".equals(wacCdxRecord.gzLen)) {
        		if (Long.parseLong(wacCdxRecord.gzLen) != jwatCdxEntry.length) {
            		throw new WeirdFileException("Lengths differ: " + wacCdxRecord.gzLen + " - " + jwatCdxEntry.length);
    		    }
    		}
    		*/
    	}
    	if ((jwatCdxEntries.size() - nulls) > wacCdxRecords.size()) {
    		logger.warn("JWAT parsed more records than WAC. (" + (jwatCdxEntries.size() - nulls) + " > " + wacCdxRecords.size() + ")");
    	}
    	if (disturbances > 0) {
    		logger.warn("{} disturbances registered while parsing file.", disturbances);
    	}
    	if (nulls > 0) {
    		logger.warn("{} null records reported by WAC.", nulls);
    	}
        if (disturbances > 10) {
        	throw new WeirdFileException(String.format("%d disturbances registered!", 10));
        }
    	if (wacCdxRecords.size() - wIdx > 1) {
    		throw new WeirdFileException(String.format("WAC parsed records after JWAT stopped. (%d/%d > %d/%d)!", wIdx, wacCdxRecords.size(), jIdx, jwatCdxEntries.size()));
    	}
    	// Find last WAC non null record.
		wacCdxRecord = null;
		int lIdx = wacCdxRecords.size() - 1;
		while (lIdx >= 0 && (wacCdxRecord = wacCdxRecords.get(lIdx)) == null) {
			--lIdx;
		}
		// Find last JWAT non null entry.
		jwatCdxEntry = null;
		lIdx = jwatCdxEntries.size() - 1;
		while (lIdx >= 0 && (jwatCdxEntry = jwatCdxEntries.get(lIdx)) == null) {
			--lIdx;
		}
		if (wacCdxRecord == null || jwatCdxEntry == null) {
			throw new WeirdFileException("Unable to find the last WAC and/or JWAT record/entry!");
		}
		if (Long.parseLong(wacCdxRecord.offset) > jwatCdxEntry.offset) {
			throw new WeirdFileException(String.format("WAC parsed record past where the last JWAT entry was parsed! (%d > %d)", Long.parseLong(wacCdxRecord.offset), jwatCdxEntry.offset));
		}

        if (wacCdxRecords.size() > jwatCdxEntries.size()) {
    		wacCdxRecord = wacCdxRecords.get(wacCdxRecords.size() - 1);
    		if ((wacCdxRecords.size() - nulls - 1 != jwatCdxEntries.size()) || (wacCdxRecord != null && !"-".equalsIgnoreCase(wacCdxRecord.offset) && filelength - Long.parseLong(wacCdxRecord.offset) > 100)) {
    			// Debug
    			//saveWacCdx("wac.cdx", wacCdxRecords);
    			//saveJwatCdx("jwat.cdx", jwatCdxEntries);
    			if (wacCdxRecords.size() - nulls - jwatCdxEntries.size() > 10) {
            		throw new WeirdFileException(String.format("WAC parsed more records than JWAT. (%d > %d)", wacCdxRecords.size(), jwatCdxEntries.size()));
    			}
    		}
    	}
    	return false;
    }

    public String cleanupMimetype(String mimetype) {
    	int idx;
    	int pIdx;
    	StringBuilder sb;
    	String oldMimetype;
    	if (mimetype != null) {
    		if ((idx = mimetype.indexOf("%20")) != -1) {
    			sb = new StringBuilder();
    			if (idx > 0) {
        			sb.append(mimetype.substring(0, idx));
    			}
    			sb.append(' ');
    			idx += 3;
    			pIdx = idx;
    			while ((idx = mimetype.indexOf("%20", idx)) != -1) {
        			sb.append(mimetype.substring(pIdx, idx));
        			sb.append(' ');
        			idx += 3;
        			pIdx = idx;
    			}
    			if (pIdx < mimetype.length()) {
        			sb.append(mimetype.substring(pIdx, mimetype.length()));
    			}
    			idx = 0;
    			while ((idx = sb.indexOf("  ", idx)) != -1) {
    				sb.delete(idx, idx + 1);
    				++idx;
    			}
    			while (sb.charAt(0) == ' ') {
    				sb.delete(0, 1);
    			}
    			while (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') {
    				sb.delete(sb.length() - 1, sb.length());
    			}
    			oldMimetype = mimetype;
    			mimetype = sb.toString();
    			logger.info("Mimetype massaged: {} -> {}", oldMimetype, mimetype);
    		} else if ((idx = mimetype.indexOf("  ")) != -1) {
    			sb = new StringBuilder(mimetype);
				sb.delete(idx, idx + 1);
				++idx;
    			while ((idx = sb.indexOf("  ", idx)) != -1) {
    				sb.delete(idx, idx + 1);
    				++idx;
    			}
    			while (sb.charAt(0) == ' ') {
    				sb.delete(0, 1);
    			}
    			while (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') {
    				sb.delete(sb.length() - 1, sb.length());
    			}
    			oldMimetype = mimetype;
    			mimetype = sb.toString();
    			logger.info("Mimetype massaged: {} -> {}", oldMimetype, mimetype);
    		} else {
    			oldMimetype = mimetype;
    			mimetype = mimetype.trim();
    			if (!mimetype.equalsIgnoreCase(oldMimetype)) {
        			logger.info("Mimetype massaged: {} -> {}", oldMimetype, mimetype);
    			}
    		}
    	}
    	return mimetype;
    }

    /**
     * Debug output webarchive-commons CDX data.
     * @param filename
     * @param wacCdxRecords
     */
	public void saveWacCdx(String filename, List<CDXRecord> wacCdxRecords) {
		RandomAccessFile raf = null;
		StringBuilder sb = new StringBuilder();
		try {
			raf = new RandomAccessFile(filename, "rw");
			Iterator<CDXRecord> iter = wacCdxRecords.iterator();
			CDXRecord record;
			while (iter.hasNext()) {
				record = iter.next();
				sb.setLength(0);
				if (record == null) {
					sb.append("null");
				} else {
					sb.append(record.offset);
					sb.append(" ");
					sb.append(record.mime);
					sb.append(" ");
					sb.append(record.origUrl);
				}
				sb.append("\n");
				raf.write(sb.toString().getBytes("UTF-8"));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(raf);
		}
	}

	/**
     * Debug output JWAT CDX data.
	 * @param filename
	 * @param jwatCdxEntries
	 */
	public void saveJwatCdx(String filename, List<CDXEntry> jwatCdxEntries) {
		RandomAccessFile raf = null;
		StringBuilder sb = new StringBuilder();
		try {
			raf = new RandomAccessFile(filename, "rw");
			Iterator<CDXEntry> iter = jwatCdxEntries.iterator();
			CDXEntry entry;
			while (iter.hasNext()) {
				entry = iter.next();
				sb.setLength(0);
				if (entry == null) {
					sb.append("null");
				} else {
					sb.append(entry.offset);
					sb.append(" ");
					sb.append(entry.mimetype);
					sb.append(" ");
					sb.append(entry.url);
				}
				sb.append("\n");
				raf.write(sb.toString().getBytes("UTF-8"));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(raf);
		}
	}

}
