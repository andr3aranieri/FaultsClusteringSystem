package faultsclusteringsystem.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.PreparedStatement;

import faultsclusteringsystem.entity.User;

public class UserDB {

	private DBManager dbManager = new DBManager();

	public User getUser(int idUser) throws ClassNotFoundException, SQLException {
		User ret = null;
		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("select * from User where idUser=?");
			stmt.setInt(1, idUser);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				ret = this.readUser(rs);
			}
			return ret;
		} finally {
			conn.close();
		}
	}

	public boolean updateK(int idUser, int k) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		int ret = 0;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("UPDATE User set k=? where idUser=?");
			stmt.setInt(1, k);
			stmt.setInt(2, idUser);
			ret = stmt.executeUpdate();
		}
		finally {
			conn.close();
		}
		
		return ret > 0;
	}

	public boolean updateClusteringState(int idUser, String newState) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		int ret = 0;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("UPDATE User SET clusteringState=? WHERE idUser=?");
			stmt.setString(1, newState);
			stmt.setInt(2, idUser);
			ret = stmt.executeUpdate();
		}
		finally {
			conn.close();
		}
		
		return ret > 0;
	}

	private User readUser(ResultSet rs) throws SQLException {
		User ret = new User();
		ret.setIdUser(rs.getInt(1));
		ret.setUser(rs.getString(2));
		ret.setDistanceMeasure(rs.getString(3));
		ret.setK(rs.getInt(4));
		ret.setClusteringState(rs.getString(5));
		ret.setTreshold(rs.getFloat(6));
		ret.setHistoricalClusteringState(rs.getString(7));
		return ret;
	}

}
