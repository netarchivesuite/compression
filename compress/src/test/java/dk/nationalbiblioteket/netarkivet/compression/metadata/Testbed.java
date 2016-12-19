package dk.nationalbiblioteket.netarkivet.compression.metadata;

import dk.netarkivet.common.utils.SystemUtils;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriter;
import dk.netarkivet.harvester.harvesting.metadata.MetadataFileWriterWarc;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by csr on 12/19/16.
 */
public class Testbed {

    String input_warc = "src/test/data/10-metadata-1.warc";
    String output_warc_dir_S = "src/test/working";
    File output_warc_dir = new File(output_warc_dir_S);

    @BeforeTest
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(output_warc_dir);
        FileUtils.forceMkdir(output_warc_dir);
    }

    @Test
    public void testReadWrite() throws IOException {
        File output_file = new File(output_warc_dir, "output.warc.gz");
        MetadataFileWriter writer = MetadataFileWriter.createWriter(output_file);


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

    public static String valueOrNull(HeaderLine line) {
        if (line == null) {
            return null;
        } else {
            return line.value;
        }
    }

}
