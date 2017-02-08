package dk.nationalbiblioteket.netarkivet.compression.tools;

import dk.netarkivet.archive.arcrepositoryadmin.Admin;
import dk.netarkivet.archive.arcrepositoryadmin.AdminFactory;
import dk.netarkivet.common.distribute.Channels;
import dk.netarkivet.common.distribute.arcrepository.Replica;
import dk.netarkivet.common.distribute.arcrepository.ReplicaStoreState;
import dk.netarkivet.common.utils.KeyValuePair;
import dk.netarkivet.common.utils.batch.ChecksumJob;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 *
 */
public class CreateAdminDatabase {

    public static void main(String[] args) throws IOException {
        File checksumFile = new File(args[0]);
        Admin admin = AdminFactory.getInstance();
        Collection<Replica> replicas = Replica.getKnown();
        for (String line: FileUtils.readLines(checksumFile) ) {
            KeyValuePair<String, String> parsedLine =  ChecksumJob.parseLine(line);
            String filename = parsedLine.getKey();
            String checksum = parsedLine.getValue();
            admin.addEntry(filename, null, checksum);
            for (Replica replica: replicas) {
                admin.setState(filename, Channels.retrieveReplicaChannelNameFromReplicaId(replica.getId()), ReplicaStoreState.UPLOAD_COMPLETED);
            }
        }
    }

}
