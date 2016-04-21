package faultsclusteringsystem.jobs.silhouette.pojo;

import faultsclusteringsystem.jobs.utility.MyVector;

public class PointData {
	
	private String docID;
	private MyVector vector;
	private String cluster;
	private double avgClusterDistance;
	
	public String getDocID() {
		return docID;
	}
	public void setDocID(String docID) {
		this.docID = docID;
	}
	public MyVector getVector() {
		return vector;
	}
	public void setVector(MyVector vector) {
		this.vector = vector;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	public double getAvgClusterDistance() {
		return avgClusterDistance;
	}
	public void setAvgClusterDistance(double avgClusterDistance) {
		this.avgClusterDistance = avgClusterDistance;
	}
	
}
