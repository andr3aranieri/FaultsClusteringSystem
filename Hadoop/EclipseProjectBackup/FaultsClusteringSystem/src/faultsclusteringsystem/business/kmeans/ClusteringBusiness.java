package faultsclusteringsystem.business.kmeans;

import java.io.IOException;
import java.sql.SQLException;

import faultsclusteringsystem.entity.User;

public class ClusteringBusiness {
	
	private IndexCreation indexCreation = new IndexCreation();
	private KMeans kMeans = new KMeans();
	
	public void kmeansExe(User user) throws ClassNotFoundException, SQLException, IOException, InterruptedException {

		System.out.println("*********************************************************");
		System.out.println("*********************************************************");
		System.out.println("*****************CLUSTERING EXECUTION********************");
		System.out.println("*********************************************************");
		
		System.out.println("Clustering Execution> create today documents index");
		
		this.indexCreation.createIndexToday(user);

		System.out.println("Clustering Execution> compute k means clustering");
		
		this.kMeans.kMeansToday(user);

		System.out.println("*********************************************************");
		System.out.println("**********************END CLUSTERING*********************");
		System.out.println("*********************************************************");
		System.out.println("*********************************************************");

	}
}
