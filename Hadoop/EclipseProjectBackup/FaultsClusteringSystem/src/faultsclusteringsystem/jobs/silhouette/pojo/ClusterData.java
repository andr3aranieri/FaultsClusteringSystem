package faultsclusteringsystem.jobs.silhouette.pojo;

import faultsclusteringsystem.jobs.utility.MyVector;

public class ClusterData {
	
	private String cluster;
	private MyVector sumVector;
	private int numPoints;
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	public MyVector getSumVector() {
		return sumVector;
	}
	public void setSumVector(MyVector sumVector) {
		this.sumVector = sumVector;
	}
	public int getNumPoints() {
		return numPoints;
	}
	public void setNumPoints(int numPoints) {
		this.numPoints = numPoints;
	}
	
}
