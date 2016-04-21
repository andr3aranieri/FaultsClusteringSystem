package faultsclusteringsystem.business.estimation;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import faultsclusteringsystem.entity.User;
import faultsclusteringsystem.manager.ClusterManager;
import faultsclusteringsystem.manager.DocumentManager;
import faultsclusteringsystem.manager.HadoopManager;

public class KMeansEstimation {
	private HadoopManager hadoopManager = new HadoopManager();
	private DocumentManager documentManager = new DocumentManager();
	private ClusterManager clusterManager = new ClusterManager();
	
	public static final String JOBNAME = "KMEANS> ";

	public void kMeansToday(User user) throws ClassNotFoundException, IOException, InterruptedException, SQLException {
		File f = this.documentManager.getTodayTmpVectors(user.getIdUser());

		// each user has an input dir and an output dir;
		String inputDir = "/FinalProject/KMeansEstimation/input_" + user.getIdUser();
		String outputDir = "/FinalProject/KMeansEstimation/output_" + user.getIdUser();
		String centersPath = "/FinalProject/KMeansEstimation/centers_" + user.getIdUser();
		
		this.hadoopManager.tryToCreateDirectory(inputDir);
		this.hadoopManager.tryToCreateDirectory(centersPath);
		
		System.out.println(JOBNAME + "Write documents to HDFS...");
		// write raw documents to HDFS IndexCreation job input directory;
		this.hadoopManager.writeFileToHDFS(f.getAbsolutePath(), inputDir);

		System.out.println(JOBNAME + "Delete output directory...");
		this.hadoopManager.deleteHDFSDirectory(outputDir);
		
		String lastOutputDir = this.hadoopManager.launchKMeans(user, inputDir, outputDir, centersPath, "TmpCluster");
		
		//delete old clusters
		this.clusterManager.deleteVolatileClusters(user.getIdUser());
		
		//write keymeans output to DB;
		this.hadoopManager.readInsertsFromHDFSToDB(lastOutputDir);
	}
}
