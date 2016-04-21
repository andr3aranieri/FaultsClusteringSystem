package faultsclusteringsystem.db;

import java.sql.DriverManager;
import java.sql.SQLException;
import com.mysql.jdbc.Connection;

public class DBManager {

	// JDBC driver name and database URL
	final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	final String DB_URL = "jdbc:mysql://localhost/ClusteringSystem";

	// Database credentials
	final String USER = "root";
	final String PASS = "andrea";

	public Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = (Connection) DriverManager.getConnection(this.DB_URL, this.USER, this.PASS);
		return conn;
	}

	public String getJDBC_DRIVER() {
		return JDBC_DRIVER;
	}

	public String getDB_URL() {
		return DB_URL;
	}

	public String getUSER() {
		return USER;
	}

	public String getPASS() {
		return PASS;
	}
}
