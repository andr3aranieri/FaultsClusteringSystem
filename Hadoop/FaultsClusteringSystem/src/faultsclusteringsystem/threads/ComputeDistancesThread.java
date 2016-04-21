package faultsclusteringsystem.threads;

import java.sql.SQLException;

import faultsclusteringsystem.business.estimation.KEstimation;
import faultsclusteringsystem.entity.User;

public class ComputeDistancesThread implements Runnable {

	private KEstimation kEstimation = new KEstimation();
	private User user = null;
	
	public ComputeDistancesThread(User user) {
		this.user = user;
	}
	
	@Override
	public void run() {
		
		int seconds = 600;
		
		while (true)
		{
			System.out.println("KEstimation.DistanceMeasureThread> compute distances...");

			try {
				this.kEstimation.computeDistances(user);
			} catch (ClassNotFoundException | SQLException e1) {
				e1.printStackTrace();
			}
			
			System.out.println("KEstimation.DistanceMeasureThread> ...done. Sleep for " + seconds + " seconds");
			try {
				Thread.sleep(seconds * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
