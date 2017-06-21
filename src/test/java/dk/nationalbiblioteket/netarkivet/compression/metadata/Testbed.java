package dk.nationalbiblioteket.netarkivet.compression.metadata;

import com.google.common.primitives.Bytes;
import dk.netarkivet.common.utils.Settings;
import dk.netarkivet.harvester.HarvesterSettings;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriter;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriterWarc;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcReaderUncompressed;
import org.jwat.arc.ArcRecord;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.ANVLRecord;
import org.jwat.common.HeaderLine;
import org.jwat.common.Payload;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import static dk.netarkivet.harvester.HarvesterSettings.*;
import static dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriter.*;

/**
 * Created by csr on 12/19/16.
 */
public class Testbed {

    String input_warc = "src/test/data/WORKING/10-metadata-1.warc";
    String input_arc = "src/test/data/WORKING/3-metadata-1.arc";
    String special_arc = "src/test/data/WORKING/temp.arc";
    String output_dir_S = "src/test/working";
    File output_dir = new File(output_dir_S);

    //@BeforeTest
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(output_dir);
        FileUtils.forceMkdir(output_dir);
        MetadatafileGeneratorRunnableTest.setup();
    }

    //@Test
    public void testIterateOverProblemFile() throws IOException {
        File input = new File(special_arc);
        InputStream is = new FileInputStream(input);
        ArcReader reader = ArcReaderFactory.getReader(is);
        final Iterator<ArcRecordBase> iterator = reader.iterator();
        while (iterator.hasNext()) {
            ArcRecordBase recordBase = iterator.next();
            //System.out.println("Reading record " + recordBase.getUrlStr());
            byte[] payload = new byte[]{};
            if (recordBase.hasPayload()) {
                try {
                    payload = IOUtils.toByteArray(recordBase.getPayload().getInputStreamComplete());
                } catch (IOException e) {
                    System.out.println(e.getMessage());                    
                }
            }
        }
        reader.close();
    }

    //@Test
    public void testReadWriteWarc() throws IOException {
        File output_file = new File(output_dir, (new File(input_warc)).getName()+".gz");
        MetadataFileWriter writer = createWriter(output_file);


        InputStream is = Files.newInputStream(Paths.get(input_warc));
        WarcReader reader = WarcReaderFactory.getReader(is);
        final Iterator<WarcRecord> iterator = reader.iterator();
        while (iterator.hasNext()) {
            WarcRecord record = iterator.next();
            if (record.getHeader(WarcConstants.FN_WARC_TYPE).value.equals("warcinfo")) {
                ANVLRecord infoPayload = new ANVLRecord();
                infoPayload.addLabelValue("replaces", record.getHeader(WarcConstants.FN_WARC_FILENAME).value);
                infoPayload.addValue(IOUtils.toString(record.getPayloadContent()));
                ((MetadataFileWriterWarc) writer).insertInfoRecord(infoPayload);

            } else if (record.getHeader(WarcConstants.FN_WARC_TYPE).value.equals("resource")) {
                final HeaderLine uriLine = record.getHeader(WarcConstants.FN_WARC_TARGET_URI);
                final HeaderLine contentTypeLine = record.getHeader(WarcConstants.FN_CONTENT_TYPE);
                final HeaderLine hostIpLine = record.getHeader(WarcConstants.FN_WARC_IP_ADDRESS);
                byte[] payloadBytes = new byte[]{};
                if (record.hasPayload()) {
                    Payload payload = record.getPayload();
                    payloadBytes = IOUtils.toByteArray(payload.getInputStreamComplete());
                }
                writer.write(valueOrNull(uriLine),
                        valueOrNull(contentTypeLine),
                        valueOrNull(hostIpLine), System.currentTimeMillis(), payloadBytes);
            }
        }
        writer.close();
    }

    //@Test
    public void testReadWriteArc() throws IOException {
        File output_file = new File(output_dir, (new File(input_arc)).getName() + ".gz" );
        Settings.set(METADATA_FORMAT, "arc");
        MetadataFileWriter writer = createWriter(output_file);
        InputStream is = Files.newInputStream(Paths.get(input_arc));
        ArcReader reader = ArcReaderFactory.getReader(is);
        final Iterator<ArcRecordBase> iterator = reader.iterator();
        while (iterator.hasNext()) {
            ArcRecordBase recordBase = iterator.next();
            byte[] payload = new byte[]{};
            if (recordBase.hasPayload()) {
                payload = IOUtils.toByteArray(recordBase.getPayloadContent());
            }
            writer.write(recordBase.getUrlStr(), recordBase.getContentTypeStr(), recordBase.getIpAddress(), System.currentTimeMillis(), payload);
        }
        writer.close();
    }

    /**
     * TODO
     * Problem here is that the original ArcRecord headers are not kept. The fix would be to add a method to MetadaFileWriterWarc that
     * takes custom headers.
     * @throws IOException
     */
    public void testReadArcWriteWarc() throws IOException {
        File output_file = new File(output_dir, (new File(input_arc)).getName() + ".warc.gz" );
        Settings.set(METADATA_FORMAT, "warc");
        MetadataFileWriter writer = createWriter(output_file);
        ANVLRecord warcInfoRecord = new ANVLRecord();
        warcInfoRecord.addLabelValue("replaces", (new File(input_arc)).getName());
        ((MetadataFileWriterWarc) writer).insertInfoRecord(warcInfoRecord);
        InputStream is = Files.newInputStream(Paths.get(input_arc));
        ArcReader reader = ArcReaderFactory.getReader(is);
        final Iterator<ArcRecordBase> iterator = reader.iterator();
        while (iterator.hasNext()) {
            ArcRecordBase recordBase = iterator.next();
            byte[] payload = new byte[]{};
            if (recordBase.hasPayload()) {
                payload = IOUtils.toByteArray(recordBase.getPayloadContent());
            }
            writer.write(recordBase.getUrlStr(), recordBase.getContentTypeStr(), recordBase.getIpAddress(), System.currentTimeMillis(), payload);
        }
        writer.close();
    }


    public static String valueOrNull(HeaderLine line) {
        if (line == null) {
            return null;
        } else {
            return line.value;
        }
    }

}
