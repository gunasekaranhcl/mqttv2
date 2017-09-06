package com.mqtt;

import java.sql.*;

import org.h2.jdbcx.JdbcConnectionPool;

public class DataStore {

	static final String JDBC_DRIVER = "org.h2.Driver";
	static final String DB_URL = "jdbc:h2:~/hearts";

	static final String USER = "sa";
	static final String PASS = "sa";

	JdbcConnectionPool cp = JdbcConnectionPool.create(DB_URL, USER,
			PASS);
	
	public void storeData(String query){

		Connection conn;
		try {
			conn = cp.getConnection();		
			conn.createStatement().execute(query);
			conn.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		

	}

	public void executeSQLQuery(String query) {
		Connection conn = null;
		Statement stmt = null;
		try {

			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt.executeUpdate(query);

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					conn.close();
			} catch (SQLException se) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}

	}

	public void createTable() {
		String sql = "CREATE TABLE IF NOT EXISTS HEARTBEAT "
				+ "(id INTEGER auto_increment, " + " pulse BOOLEAN, "
				+ " duration INTEGER, "
				+ " time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
				+ " topic varchar(255) , "
				+ " message varchar(255) , "
				+ " bpm INTEGER)";
		executeSQLQuery(sql);
	}

	public void deleteTable() {
		String sql = "DROP TABLE HEARTBEAT;";
		executeSQLQuery(sql);
	}

	public void insertData(String data) {

		String[] datavalues = data.split("/");
		for (int i = 0; i < datavalues.length; i++) {
			String[] cellvalues = datavalues[i].split(";");
			int pusle = 0;
			if (cellvalues[0].contains("true")) {
				pusle = 1;
			}

			String sql = "INSERT INTO HEARTBEAT(pulse,duration,bpm) "
					+ "VALUES (" + pusle + ",1,60)";
			executeSQLQuery(sql);

		}
	}

	public void selectTable() {
		Connection conn = null;
		Statement stmt = null;

		try {

			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM HEARTBEAT;");
			while (rs.next()) {
				int id = rs.getInt(1);
				System.out.println(id);
				boolean pulse = rs.getBoolean(2);
				System.out.println(pulse);

				int duration = rs.getInt(3);
				System.out.println(duration);

				Timestamp time = rs.getTimestamp(4);
				System.out.println(time);
			}

		} catch (SQLException se) {

			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					conn.close();
			} catch (SQLException se) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

	
}