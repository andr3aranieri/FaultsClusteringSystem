package faultsclusteringsystem.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mysql.jdbc.PreparedStatement;

import faultsclusteringsystem.entity.PersistentDocument;

public class PersistentDocumentDB {
	private DBManager dbManager = new DBManager();

	public List<PersistentDocument> getVolatileDocuments(int idUser, Date from, Date to) throws ClassNotFoundException, SQLException {
		List<PersistentDocument> ret = new ArrayList<PersistentDocument>();
		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("select * from VolatileDocument where idUser=? and insertDate >= ? and insertDate <= ?");
			stmt.setInt(1, idUser);
			stmt.setDate(2, new java.sql.Date(from.getTime()));
			stmt.setDate(3, new java.sql.Date(to.getTime()));
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ret.add(this.readPersistentDocument(rs));
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
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("update PersistentDocument set vector=? where PersistentDocument=?");
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

	private PersistentDocument readPersistentDocument(ResultSet rs) throws SQLException {
		PersistentDocument ret = new PersistentDocument();
		ret.setIdPersistentDocument(rs.getInt(0));
		ret.setIdUser(rs.getInt(1));
		ret.setRawData(rs.getString(2));
		ret.setVector(rs.getString(3));
		ret.setInsertDate(rs.getDate(4));
		return ret;
	}
}
