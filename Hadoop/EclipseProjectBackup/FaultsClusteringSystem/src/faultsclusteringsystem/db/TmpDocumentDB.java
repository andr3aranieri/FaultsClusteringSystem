package faultsclusteringsystem.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import faultsclusteringsystem.entity.TmpDocument;
import faultsclusteringsystem.entity.VolatileDocument;

public class TmpDocumentDB {

	private DBManager dbManager = new DBManager();

	public boolean copyVolatileDocuments(int idUser) throws SQLException {
		boolean ret = true;

		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			conn.setAutoCommit(false);

			// delete old tmpClusters;
			PreparedStatement stmt0 = (PreparedStatement) conn
					.prepareStatement("DELETE FROM TmpCluster WHERE idUser=?");
			stmt0.setInt(1, idUser);
			stmt0.executeUpdate();

			// delete old tmpDocuments;
			PreparedStatement stmt = (PreparedStatement) conn
					.prepareStatement("DELETE FROM TmpDocument WHERE idUser=?");
			stmt.setInt(1, idUser);
			stmt.executeUpdate();

			// bulk insert volatile documents in tmpdocuments;
			PreparedStatement stmt2 = (PreparedStatement) conn.prepareStatement(
					"INSERT INTO TmpDocument(idUser, rawData, vector, insertDate) SELECT idUser, rawData, vector, insertDate FROM VolatileDocument WHERE idUser = ?");
			stmt2.setInt(1, idUser);
			stmt2.executeUpdate();

			conn.commit();
		} catch (Exception ex) {
			conn.rollback();
			ex.printStackTrace();
		} finally {
			conn.close();
		}

		return ret;
	}
	
	public List<TmpDocument> getTmpDocuments(int idUser) throws ClassNotFoundException, SQLException {
		List<TmpDocument> ret = new ArrayList<TmpDocument>();
		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("select * from TmpDocument where idUser=?");
			stmt.setInt(1, idUser);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ret.add(this.readTmpDocument(rs));
			}
		} finally {
			conn.close();
		}
		return ret;
	}

	public HashMap<String, String> getTmpDocumentsClusters(int idUser) throws SQLException, ClassNotFoundException {
		HashMap<String, String> ret = new HashMap<String, String>();
		Connection conn = null;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("SELECT d.idTmpDocument, c.center FROM TmpDocument d JOIN TmpCluster c ON d.idTmpDocument = c.idDocument where d.idUser=?");
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

	private TmpDocument readTmpDocument(ResultSet rs) throws SQLException {
		TmpDocument ret = new TmpDocument();
		ret.setIdTmpDocument(rs.getInt(1));
		ret.setIdUser(rs.getInt(2));
		ret.setRawData(rs.getString(3));
		ret.setVector(rs.getString(4));
		ret.setInsertDate(rs.getDate(5));
		return ret;
	}
}
