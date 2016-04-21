package faultsclusteringsystem.entity;

public class Distance {

	private int idDistance;
	private int idUser;
	private int point1;
	private int point2;
	private double distance;
	private boolean read;

	public int getIdDistance() {
		return idDistance;
	}

	public void setIdDistance(int idDistance) {
		this.idDistance = idDistance;
	}

	public int getIdUser() {
		return idUser;
	}

	public void setIdUser(int idUser) {
		this.idUser = idUser;
	}

	public int getPoint1() {
		return point1;
	}

	public void setPoint1(int point1) {
		this.point1 = point1;
	}

	public int getPoint2() {
		return point2;
	}

	public void setPoint2(int point2) {
		this.point2 = point2;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public boolean isRead() {
		return read;
	}

	public void setRead(boolean read) {
		this.read = read;
	}

}
