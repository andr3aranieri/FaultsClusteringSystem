package faultsclusteringsystem.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.mysql.jdbc.Statement;

public class GenericDB {
	private DBManager dbManager = new DBManager();

	public boolean bulkUpdate(List<String> queries) throws SQLException {
		boolean ret = true;
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = this.dbManager.getConnection();
			conn.setAutoCommit(false);
			stmt = (Statement) conn.createStatement();
			for(String query: queries) {
				stmt.addBatch(query);
			}
			stmt.executeBatch();
			conn.commit();
		}
		catch(Exception ex) {
			ex.printStackTrace();
			conn.rollback();
			ret = false;
		}
		finally {
			conn.close();
		}
		return ret;
	}
}
