package dk.nationalbiblioteket.netarkivet.compression.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import dk.netarkivet.common.utils.archive.GetMetadataArchiveBatchJob;
import dk.netarkivet.common.utils.batch.BatchLocalFiles;

public class CheckMetadataDeduplicationMigrationRecords {

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Missing arg - filelist");
			System.exit(1);
		}
		File filelist = new File(args[0]);
		if (!filelist.exists()) {
			System.err.println("Arg filelist does not exist");
			System.exit(1);
		}
		List<String> files = org.apache.commons.io.FileUtils.readLines(filelist);
		System.out.println("Found " + files.size() + " files to investigate");
		int total = files.size();
		int filesnotfound=0;
		int filesfound=0;
		int ok=0;
		int notok=0;
		for (String f: files) {
			File file = new File(f);
			if (!file.exists()){
				filesnotfound++;
				continue;
			}
			filesfound++;
			Set<String> errors = new HashSet<String>();
			BatchLocalFiles batch = new BatchLocalFiles(new File[]{file});
			File resultFile = File.createTempFile("batch", "dedupmigtest", new File("/tmp"));
			OutputStream os = new FileOutputStream(resultFile);
			GetMetadataArchiveBatchJob job 
			= new GetMetadataArchiveBatchJob(Pattern.compile(".*duplicationmigration.*"), Pattern.compile("text/plain"));
			batch.run(job, os);
			os.close();
			List<String> migrationLines = org.apache.commons.io.FileUtils.readLines(resultFile);
			System.out.println("Found " + migrationLines.size() + " migrationlines for file '" + file.getAbsolutePath() + "'");
			for (String line: migrationLines) {
				String[] splitLine = StringUtils.split(line);
				if (splitLine.length < 3) { 
					errors.add(line);
				}
			}
			if (resultFile.exists()) {
				resultFile.delete();
			}
			if (!errors.isEmpty()) {
				System.err.println("File '" + file.getAbsolutePath() + "' has " + errors.size() + " ERRORS. The errors are: " +  StringUtils.join(errors, ","));
				notok++;
			} else {
				System.out.println("File '" + file.getAbsolutePath() + "' is OK");
				ok++;
			}
		}
		System.out.println("Statistics total/filesfound/filesnotfound/ok/notok= " +  total + "/" + filesfound + "/" + filesnotfound + "/" + ok + "/" + notok); 
	}
}

