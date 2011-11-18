package com.migration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionProvider {
	public static Connection getPgSqlConnection() {
		String driver = "org.postgresql.Driver";
		String url = "jdbc:postgresql://192.168.10.212:5432/db_trsf_test?charSet=UTF-8";
		String username = "askul";
		String password = "askul";

		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException ce) {
			ce.printStackTrace();
		} catch (SQLException se) {
			se.printStackTrace();
		}	
		return conn;
	}

	public static Connection getSqlServerConnection() {
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String url = "jdbc:sqlserver://192.168.10.119:1433;DatabaseName=askul2006";
		String username = "sa";
		String password = "sa";

		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException ce) {
			ce.printStackTrace();
		} catch (SQLException se) {
			se.printStackTrace();
		}	
		return conn;
	}

}
