package faultsclusteringsystem.business.estimation;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import faultsclusteringsystem.entity.User;
import faultsclusteringsystem.manager.ClusterManager;
import faultsclusteringsystem.manager.DocumentManager;
import faultsclusteringsystem.manager.HadoopManager;

public class Silhouette {

	private HadoopManager hadoopManager = new HadoopManager();
	private DocumentManager documentManager = new DocumentManager();
	
	private final static String JOBNAME = "Compute silhouette> ";
	
	public void computeSilhouette(User user) throws ClassNotFoundException, IOException, InterruptedException, SQLException {
		//File f = this.clusterManager.getTmpClusteredDocuments(user.getIdUser());

		// each user has an input dir and an output dir;
		String inputDir = "/FinalProject/KEstimation/Silhouette/input_" + user.getIdUser();
		String outputDir = "/FinalProject/KEstimation/Silhouette/output_" + user.getIdUser();
		
		this.hadoopManager.tryToCreateDirectory(inputDir);
		
		System.out.println(JOBNAME + "Write documents to HDFS...");

		this.documentManager.writeTmpDocumentsToJobInputDirectory(user.getIdUser(), inputDir);
		
		System.out.println(JOBNAME + "Delete output directory...");
		this.hadoopManager.deleteHDFSDirectory(outputDir);
		
		this.hadoopManager.launchSilhouetteJob(user, inputDir, outputDir, "TmpCluster");		
	}
}
