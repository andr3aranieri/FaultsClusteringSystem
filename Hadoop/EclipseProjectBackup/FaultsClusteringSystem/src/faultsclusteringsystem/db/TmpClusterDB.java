package faultsclusteringsystem.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.PreparedStatement;

import faultsclusteringsystem.view.ClusteredDocument;

public class TmpClusterDB {

	private DBManager dbManager = new DBManager();

	public boolean deleteClusters(int idUser) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		int ret = 0;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("DELETE FROM TmpCluster WHERE idUser=?");
			stmt.setInt(1, idUser);
			ret = stmt.executeUpdate();
		} finally {
			conn.close();
		}

		return ret > 0;
	}
	
	public List<ClusteredDocument> getClusteredDocuments(int idUser) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		List<ClusteredDocument> documents = new ArrayList<ClusteredDocument>();
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("SELECT vd.idVolatileDocument, vd.vector, tc.center FROM TmpCluster tc JOIN VolatileDocument vd ON tc.idDocument = vd.idVolatileDocument WHERE tc.idUser=?");
	
			stmt.setInt(1, idUser);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				documents.add(this.readClusteredDocument(rs));
			}
		}
		finally {
			conn.close();
		}
		return documents;
	}

	private ClusteredDocument readClusteredDocument(ResultSet rs) throws SQLException {
		ClusteredDocument doc = new ClusteredDocument();
		doc.setIdDocument(rs.getInt(1));
		doc.setVector(rs.getString(2));
		doc.setCenter(rs.getString(3));
		return doc;
	}

}
