package com.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PostgreSQLSide {
	
	private final Connection conn;

	public PostgreSQLSide(Connection conn) {
		this.conn = conn;
	}

	/**
	 * 根据list中的表名删除表及其数据。<br>
	 * list取自SQLServer，故只要PostgreSQL中有与SQLServer中对应的表，则删除；否则跳过。
	 * 
	 * @param tableNames 包含表名的列表
	 */
	public boolean dropExistingTable(List<String> tableNames) throws SQLException {
		
		boolean isSuccess = true;
		Statement st = conn.createStatement();
		
		try {
			st.executeUpdate(
					SqlScriptGenerator.getDropScript(tableNames, false));
		} catch (SQLException e) {
			// TODO: remove printStackTrace and println()
			e.printStackTrace();
			System.out.println(SqlScriptGenerator.getDropScript(tableNames, true));
			// Task fails if any excetpion is thrown.
			isSuccess = false;
		} finally {
			DbUtils.closeQuietly(st);
		}
		
		return isSuccess;
	}
	
	/**
	 * 建表
	 * 
	 * @param tableName
	 * @param cols
	 */
	public boolean createTable(Map<String, List<Column>> infoMap) throws SQLException{
		
		Statement st = null;
		String sql = null;
		boolean isSuccess = true;
		
		st = conn.createStatement();	
		
		Iterator<String> iter = infoMap.keySet().iterator();
		
		while(iter.hasNext()) {
			String table = iter.next();
			List<Column> cols = infoMap.get(table);
			sql = SqlScriptGenerator.getCreateScript(table, cols, false);
			
			// If exception is throws, catch it, log it, and continue.
			try {
				st.executeUpdate(sql);
			} catch (SQLException se) {
				// TODO: replace sysout with log
				System.out.println(
						SqlScriptGenerator.getCreateScript(table, cols, true));
				// Task fails if any excetpion is thrown.
				isSuccess = false;
			}
		}
		
		DbUtils.closeQuietly(st);
		
		return isSuccess;
	}
	
	/**
	 * 通过sql脚本插入数据（仅用于不包含二进制类型字段的表）
	 * 
	 * @param tableName 表名
	 * @param data 欲插入表的数据
	 * @return true 所有数据插入成功，false 数据插入失败
	 * @throws SQLException
	 */
	public boolean insertData(List<String> scripts) throws SQLException {
		
		conn.setAutoCommit(false);
		
		boolean isSuccess = true;
		
		Statement st = null;
		
		for (String script : scripts) {
			st = conn.createStatement();
			try {
				st.execute(script);
			} catch (SQLException e) {
				// TODO: replace sysout with log
				System.out.println(script);
				// Task fails if any excetpion is thrown.
				isSuccess = false;
			}
		}
		
		conn.commit();
		DbUtils.close(st);
		
		return isSuccess;
		
	}
	

	/**
	 * 通过PreparedStatement以参数的方式将数据插入到数据库
	 * 
	 * @param table
	 * @param data
	 */
	public boolean insertDataWithBin(String table, List<List<Object>> data) 
			throws SQLException {
		
		boolean isSuccess = true;
		
		PreparedStatement ps = null;
		String sql = null;
		
		for (List<Object> row : data) {
			
			if (sql == null) {
				sql = SqlScriptGenerator.getParamInsertScript(table, row.size());	
			}
			
			ps = conn.prepareStatement(sql);

			for (int i = 0; i < row.size(); i++) {
				ps.setObject(i + 1, row.get(i));
			}
			
			try {
				ps.execute();
			} catch (SQLException e) {
				// TODO: replace sysout with log
				// log here
				// Task fails if any excetpion is thrown.
				isSuccess = false;
			}
		}
		DbUtils.close(ps);
		return isSuccess;
	}
}
