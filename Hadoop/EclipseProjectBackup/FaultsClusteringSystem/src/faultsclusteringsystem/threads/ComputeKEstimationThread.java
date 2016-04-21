package faultsclusteringsystem.threads;

import java.io.IOException;
import java.sql.SQLException;

import faultsclusteringsystem.business.estimation.EstimationBusiness;
import faultsclusteringsystem.business.estimation.Silhouette;
import faultsclusteringsystem.entity.User;

public class ComputeKEstimationThread implements Runnable {

	private User user;
	int seconds = 30;

	public ComputeKEstimationThread(User user) {
		this.user = user;
	}

	@Override
	public void run() {

		EstimationBusiness estimationBusiness = new EstimationBusiness();
		
		while (true) {
			System.out.println("KEstimationThread> launch EstimationBusiness...");

			try {
				estimationBusiness.estimationExe(user);
			} catch (ClassNotFoundException | SQLException | IOException | InterruptedException e) {
				e.printStackTrace();
			}			
			
			System.out.println("KEstimationThread> done. Sleep for " + this.seconds + " seconds.");
			
			try {
				Thread.sleep(this.seconds * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
