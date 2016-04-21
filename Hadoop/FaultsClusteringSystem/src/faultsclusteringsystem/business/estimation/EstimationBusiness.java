package faultsclusteringsystem.business.estimation;

import java.io.IOException;
import java.sql.SQLException;

import faultsclusteringsystem.entity.User;
import faultsclusteringsystem.manager.DocumentManager;

public class EstimationBusiness {
	
	private IndexCreationEstimation indexCreationEstimation = new IndexCreationEstimation();
	private KEstimation kEstimation = new KEstimation();
	private Silhouette silhouette = new Silhouette();
	private KMeansEstimation kMeansEstimation = new KMeansEstimation();
	private DocumentManager documentManager = new DocumentManager();
	
	public void estimationExe(User user) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
		System.out.println("*********************************************************");
		System.out.println("*********************************************************");
		System.out.println("****************K ESTIMATION EXECUTION*******************");
		System.out.println("*********************************************************");
	
		System.out.println("K Estimation> copy volatile documents in tmp documents");
		this.documentManager.copyVolatileDocumentInTmpDocuments(user.getIdUser());
		
		System.out.println("K Estimation> create today tmp documents index");
		this.indexCreationEstimation.createIndexToday(user);
		
		System.out.println("K Estimation> compute kmeans on tmp documents");
		this.kMeansEstimation.kMeansToday(user);
		
		System.out.println("K Estimation> compute tmp clusters silhouette");
		this.silhouette.computeSilhouette(user);
		
		System.out.println("*********************************************************");
		System.out.println("*******************END K ESTIMATION**********************");
		System.out.println("*********************************************************");
		System.out.println("*********************************************************");

	}
}
