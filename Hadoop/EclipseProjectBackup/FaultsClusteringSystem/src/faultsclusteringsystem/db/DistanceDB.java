package faultsclusteringsystem.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import faultsclusteringsystem.entity.Distance;

public class DistanceDB {

	private DBManager dbManager = new DBManager();
	
	public HashMap<String, Double> getDistances(int idUser) throws ClassNotFoundException, SQLException {
		HashMap<String, Double> ret = new HashMap<String, Double>();
		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("select * from Distance where idUser=?");
			stmt.setInt(1, idUser);
			ResultSet rs = stmt.executeQuery();
			Distance d = null;
			while (rs.next()) {
				d = this.readDistance(rs);
				ret.put(d.getPoint1() + ":" + d.getPoint2(), d.getDistance());
			}
		} finally {
			conn.close();
		}
		return ret;
	}
	
	public boolean insertDistances(int idUser, String bulkInsertQuery) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		int ret = 0;
		try {
			conn = this.dbManager.getConnection();
			conn.setAutoCommit(false);
			
			//delete old distances;
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("DELETE FROM Distance WHERE idUser=?");			
			stmt.setInt(1, idUser);
			ret = stmt.executeUpdate();
			
			//bulk insert new distances;
			Statement stmt2 = (Statement) conn.createStatement();			
			stmt2.execute(bulkInsertQuery);
			
			conn.commit();
		} 
		catch(Exception ex) {
			conn.rollback();
			ex.printStackTrace();
		}
		finally {
			conn.close();
		}

		return ret > 0;		
	}
	
	private Distance readDistance(ResultSet rs) throws SQLException {
		Distance ret = new Distance();
		ret.setIdDistance(rs.getInt(1));
		ret.setIdUser(rs.getInt(2));
		ret.setDistance(rs.getDouble(3));
		ret.setRead(rs.getBoolean(4));
		ret.setPoint1(rs.getInt(5));
		ret.setPoint2(rs.getInt(6));
		return ret;
	}
}
