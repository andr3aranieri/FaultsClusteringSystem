package faultsclusteringsystem.db;

import java.sql.Connection;
import java.sql.SQLException;

import com.mysql.jdbc.PreparedStatement;

public class VolatileClusterDB {

	private DBManager dbManager = new DBManager();
	
	public boolean deleteClusters(int idUser) throws ClassNotFoundException, SQLException {
		Connection conn = null;
		int ret = 0;
		try {
			conn = this.dbManager.getConnection();
			PreparedStatement stmt = (PreparedStatement) conn.prepareStatement("DELETE FROM VolatileCluster WHERE idUser=?");
			stmt.setInt(1, idUser);
			ret = stmt.executeUpdate();
		} finally {
			conn.close();
		}

		return ret > 0;
	}

}
