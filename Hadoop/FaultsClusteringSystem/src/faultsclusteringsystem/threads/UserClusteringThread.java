package faultsclusteringsystem.threads;

import java.io.IOException;
import java.sql.SQLException;

import faultsclusteringsystem.business.kmeans.ClusteringBusiness;
import faultsclusteringsystem.entity.User;

public class UserClusteringThread implements Runnable {

	private User user;
	private ClusteringBusiness clusteringBusiness;
	private int seconds = 60;
	
	public UserClusteringThread(User user) {
		this.user = user;
	}

	@Override
	public void run() {
		this.clusteringBusiness = new ClusteringBusiness();
		
		while (true) {
			System.out.println("UserClusteringThread> launch User clustering...");

			try {
				this.clusteringBusiness.kmeansExe(user);
			} catch (ClassNotFoundException | SQLException | IOException | InterruptedException e) {
				e.printStackTrace();
			}			
			
			System.out.println("UserClusteringThread> done. Sleep for " + this.seconds + " seconds.");
			
			try {
				Thread.sleep(this.seconds * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
}
