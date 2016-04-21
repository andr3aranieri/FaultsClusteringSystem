package faultsclusteringsystem.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mysql.jdbc.PreparedStatement;

import faultsclusteringsystem.entity.VolatileDocument;

public class VolatileDocumentDB {
	private DBManager dbManager = new DBManager();

	public List<VolatileDocument> getVolatileDocuments(int idUser) throws ClassNotFoundException, SQLException {
		List<VolatileDocument> ret = new ArrayList<VolatileDocument>();
		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("select * from VolatileDocument where idUser=?");
			stmt.setInt(1, idUser);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ret.add(this.readVolatileDocument(rs));
			}
		} finally {
			conn.close();
		}
		return ret;
	}

	public HashMap<String, String> getVolatileDocumentsClusters(int idUser) throws SQLException, ClassNotFoundException {
		HashMap<String, String> ret = new HashMap<String, String>();
		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("SELECT d.idVolatileDocument, c.center FROM VolatileDocument d JOIN TmpCluster c ON d.idVolatileDocument = c.idDocument where d.idUser=?");
			stmt.setInt(1, idUser);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ret.put(rs.getInt(1) + "", rs.getString(2));
			}
		} finally {
			conn.close();
		}
		return ret;		
	}
	
	public int updateDocumentVector(int idDocument, String vector) throws SQLException, ClassNotFoundException {
		int ret = 0;
		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("update VolatileDocument set vector=? where idVolatileDocument=?");
			stmt.setString(1, vector);
			stmt.setInt(2, idDocument);
			ret = stmt.executeUpdate();
			conn.commit();
		}
		finally {
			conn.close();
		}
		return ret;
	}
	
	private VolatileDocument readVolatileDocument(ResultSet rs) throws SQLException {
		VolatileDocument ret = new VolatileDocument();
		ret.setIdVolatileDocument(rs.getInt(1));
		ret.setIdUser(rs.getInt(2));
		ret.setRawData(rs.getString(3));
		ret.setVector(rs.getString(4));
		ret.setInsertDate(rs.getDate(5));
		return ret;
	}
}
