package dk.nationalbiblioteket.netarkivet.compression;

import org.testng.annotations.Test;

import dk.netarkivet.common.utils.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.*;

/**
 * Created by csr on 1/9/17.
 */
public class UtilTest {
    @Test
    public void testGetNewMetadataFilename() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Util.METADATA_GENERATION, "4");
        String file = "/hello/world/456-metadata-2.warc";
        Util.properties = properties;
        String output = Util.getNewMetadataFilename(file);
        assertEquals(output, "456-metadata-4.warc.gz");
        file = "/hello/world/456-metadata-2.arc";
        output = Util.getNewMetadataFilename(file);
        assertEquals(output, "456-metadata-4.arc.gz");
    }

    @Test 
    public void testGetFilteredList() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        String blacklistFilename = "blacklisted_metadatafiles.txt"; // Located physically in src/main/resources/blacklisted_metadatafiles.txt
        File file = new File(classLoader.getResource(blacklistFilename).getFile());
        assertTrue(file.exists(), "blacklist '" + file.getAbsolutePath() + "' does not exist");
        List<String> blacklist = Util.getAllLines(file.getAbsolutePath());
        assertEquals(blacklist.size(), 9775, "blacklist should have size 9775");
        List<String> filteredList = Util.getFilteredList(file.getAbsolutePath(), blacklist);
        assertEquals(filteredList.size(), 0, "FilteredList should be 0");
        List<String> collection = new ArrayList<String>();
        for (int i=0; i<10; i++) {
            collection.add(i + ".arc");
        }
        collection.add("1025-metadata-1.arc"); // should be filtered away when using the full blacklist
        
        File testFile = File.createTempFile("test", "txt");
        FileUtils.writeCollectionToFile(testFile, collection);
        filteredList = Util.getFilteredList(testFile.getAbsolutePath(), blacklist);
        List<String> emptyBlacklist = new ArrayList<String>();
        assertEquals(filteredList.size(), 10); // the eleventh is filtered out
        filteredList = Util.getFilteredList(testFile.getAbsolutePath(), emptyBlacklist);
        assertEquals(filteredList.size(), 11);// the eleventh is not filtered out
        testFile.delete();
    }
}
