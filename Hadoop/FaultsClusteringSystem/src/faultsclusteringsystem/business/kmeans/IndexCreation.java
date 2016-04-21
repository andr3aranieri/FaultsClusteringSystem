package faultsclusteringsystem.business.kmeans;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import faultsclusteringsystem.entity.User;
import faultsclusteringsystem.manager.DocumentManager;
import faultsclusteringsystem.manager.HadoopManager;

public class IndexCreation {
	private DocumentManager documentManager = new DocumentManager();
	private HadoopManager hadoopManager = new HadoopManager();

	public static final String JOBNAME = "IndexCreation> ";

	public void createIndexToday(User user) throws ClassNotFoundException, SQLException, IOException, InterruptedException {

		System.out.println(JOBNAME + "Read documents from DB...");
		// Read raw documents from DB;
		File f = this.documentManager.getTodayDocuments(user.getIdUser());

		// each user has an input dir and an output dir;
		String inputDir = "/FinalProject/IndexCreation/input_" + user.getIdUser();
		String tfidfOutputDir = "/FinalProject/IndexCreation/output_" + user.getIdUser();
		String indexOutputDir = "/FinalProject/IndexCreation/outputindex_" + user.getIdUser();
		
		this.hadoopManager.tryToCreateDirectory(inputDir);
		
		System.out.println(JOBNAME + "Write documents to HDFS...");
		// write raw documents to HDFS IndexCreation job input directory;
		this.hadoopManager.writeFileToHDFS(f.getAbsolutePath(), inputDir);

		System.out.println(JOBNAME + "Delete old tfidf output directory...");
		this.hadoopManager.deleteHDFSDirectory(tfidfOutputDir);

		System.out.println(JOBNAME + "Launch tfidf matrix creation job...");
		this.hadoopManager.launchTfidfMappingJob(inputDir, tfidfOutputDir);

		System.out.println(JOBNAME + "Delete old index output directory...");
		this.hadoopManager.deleteHDFSDirectory(indexOutputDir);
		System.out.println(JOBNAME + "Launch index creation creation job...");
		this.hadoopManager.launchIndexCreationJob(tfidfOutputDir, indexOutputDir, "VolatileDocument");

		System.out.println(JOBNAME + "Make bulk update to DB...");
		this.hadoopManager.readFilesFromHDFSToDB(indexOutputDir);
	}
}
