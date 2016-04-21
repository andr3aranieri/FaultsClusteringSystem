package faultsclusteringsystem.gui;

import java.io.IOException;
import java.sql.SQLException;

import faultsclusteringsystem.business.estimation.Silhouette;
import faultsclusteringsystem.business.kmeans.IndexCreation;
import faultsclusteringsystem.business.kmeans.KMeans;
import faultsclusteringsystem.entity.User;
import faultsclusteringsystem.manager.HadoopManager;
import faultsclusteringsystem.manager.UserManager;
import faultsclusteringsystem.threads.ComputeDistancesThread;
import faultsclusteringsystem.threads.ComputeKEstimationThread;
import faultsclusteringsystem.threads.UserClusteringThread;

public class StartSystem {

	
	
	public static void main (String[] argv) throws ClassNotFoundException, SQLException, IOException, InterruptedException {
		
		User user = new UserManager().getUser(1); 
		
		System.out.println("GUI> Starting System...");			
		
		//START THREADS
		
		System.out.println("GUI> Start compute distances thread...");
		ComputeDistancesThread computeDistancesThread = new ComputeDistancesThread(user);
		Thread t = new Thread(computeDistancesThread);
		t.start();

		/*
		System.out.println("GUI> Start K Estimation thread...");
		ComputeKEstimationThread kEstimationThread = new ComputeKEstimationThread(user);
		Thread t2 = new Thread(kEstimationThread);
		t2.start();
*/
		
		System.out.println("GUI> Start User Clustering thread...");
		UserClusteringThread userClusteringThread = new UserClusteringThread(user);
		Thread t3 = new Thread(userClusteringThread);
		t3.start();

		//START GUI LOOP			
		while(true) {
			System.out.println("GUI> heartbeat");
			
			Thread.sleep(10 * 1000);
		}

	}
	
}
