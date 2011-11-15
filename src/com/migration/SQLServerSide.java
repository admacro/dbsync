package com.migration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;

public class SQLServerSide {

	private final Connection conn;
	private List<Column> cols = new ArrayList<Column>();
	private List<String> scripts = null;
	private List<List<Object>> data = null;

	public SQLServerSide(Connection conn) {
		this.conn = conn;
	}

	/**
	 * 获取SqlServer数据库中所有表的表名
	 * 
	 * @return list 包含数据库中所有表名的list
	 * @throws SQLException
	 */
	public List<String> getTableNamesList() {
		return getTables();
	}
	
	private List<String> getTables() {
		String[] tableNamesArray = null;
		ResultSet rs = null;
		
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			rs = dbmd.getTables(null, null, "%", new String[] { "TABLE" });
			
			tableNamesArray = new String[RSHelper.getRowCount(rs)];
			
			int i = 0;
			while (rs.next()) {
				// 第三列为表名
				String tableName = rs.getString(3).toLowerCase();
				tableNamesArray[i++] = tableName;
			}
			
		} catch (SQLException se) {
			// TODO: remove printStackTrace()
			se.printStackTrace();
		} finally {
			DbUtils.closeQuietly(rs);
		}
		
		return Arrays.asList(tableNamesArray);
	}
	
	/**
	 * 根据SqlServer数据库中表的表名获取该表所有字段的信息
	 * 
	 * @param tableName
	 *            欲获取字段的表的表名
	 * @return list 包含该表所有字段信息的list
	 * @throws SQLException
	 */
	public List<Column> getColumnsByTableName(String tableName) {
		return getColumns(tableName);
	}
	
	private List<Column> getColumns(String tableName) {
		cols.clear();
		ResultSet rs = null;
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			rs = dbmd.getColumns(null, null, tableName, "%");
			while (rs.next()) {
				Column column = new Column();
				column.setName(ColumnHelper.quoteColumnName(rs.getString("COLUMN_NAME")));
				column.setSqlType(rs.getString("TYPE_NAME"));
				column.setLength(rs.getInt("COLUMN_SIZE"));
				column.setPrecision(rs.getInt("DECIMAL_DIGITS"));
				cols.add(column);
			}
		} catch (SQLException se) {
			// TODO: remove printStackTrace()
			se.printStackTrace();
		} finally {
			DbUtils.closeQuietly(rs);
		}
		return cols;
	}
	
	/**
	 * 获取建表所需数据（表名及每张表对应的所有列的信息）
	 * 
	 * @return 所有建表需要的数据
	 */
	public HashMap<String, List<Column>> getInfoMap() {
		HashMap<String, List<Column>> infoMap = new HashMap<String, List<Column>>();
		for (String table : getTables()) {
			infoMap.put(table, getColumns(table));
		}
		return infoMap;
	}

	/**
	 * 获取表中的所有数据
	 * 
	 * @param table 表名
	 * @param binary 是否包含二进制类型的字段
	 * @return 此表的所有数据
	 * @throws SQLException
	 */
	public List<List<Object>> getDataWithBin(String table) 
			throws SQLException {
		if (data != null && data.size() > 0) {
			data.clear();
		}
		data = new ArrayList<List<Object>>(getSelectCount(table));
		cols = getColumns(table);
		List<Object> row = null;
		
		ResultSet rs = null;
		String sql = SqlScriptGenerator.getSelectAll(table);
		rs = conn.createStatement().executeQuery(sql);
		
		while (rs.next()) {
			row = new ArrayList<Object>(cols.size());
			for (int i = 0; i < cols.size(); i++) {
				if (ColumnHelper.isBinary(cols.get(i))) {
					row.add(rs.getBytes(i + 1));
				} else {
					row.add(rs.getObject(i + 1));
				}
			}
			data.add(row);
			row.clear();
			row = null;
		}
		
		cols.clear();
		
		DbUtils.closeQuietly(rs);
		return data;
	}
	
	/**
	 * 获取数据插入的sql脚本
	 * 
	 * @param table 表名
	 * @return 该表的数据插入语句
	 * @throws SQLException
	 */
	public List<String> getInsertScripts(String table) throws SQLException {
		
		if (scripts != null && scripts.size() > 0) {
			scripts.clear();
			scripts = null;
		}
		scripts = new ArrayList<String>(getSelectCount(table));
		cols = getColumns(table);
		List<Object> row = null;
		
		ResultSet rs = null;
		String sql = SqlScriptGenerator.getSelectAll(table);
		rs = conn.createStatement().executeQuery(sql);
		
		while (rs.next()) {
			row = new ArrayList<Object>(cols.size());
			for (int i = 0; i < cols.size(); i++) {
				row.add(rs.getObject(i + 1));
			}
			scripts.add(SqlScriptGenerator.getInsertScript(table, row, false));
			row.clear();
			row = null;
		}
		
		DbUtils.closeQuietly(rs);
		
		sql = null;
		cols.clear();
		
		return scripts;
		
	}

	/**
	 * 检查表是否包含二进制类型的字段(binary, image 或 varbinary)
	 * 
	 * @param table 表名
	 * @return true 包含，false 不包含
	 */
	public boolean containsBinCol(String table) {
		cols = getColumns(table);
		for (Column c : cols) {
			if(ColumnHelper.isBinary(c)) {
				return true;
			}
		}
		cols.clear();
		return false;
	}
	
	/**
	 * 检查表中是否有数据
	 * 
	 * @param table 表名
	 * @return true 表中无数据，false 表中有数据
	 * @throws SQLException
	 */
	public boolean isEmpty(String table) throws SQLException {
		return getSelectCount(table) > 0 ? false : true;
	}
	
	/**
	 * 获取表中的记录数
	 * 
	 * @param table 表名
	 * @return 表中的记录数
	 * @throws SQLException
	 */
	public int getSelectCount(String table) throws SQLException {
		int cnt = 0;
		
		String sql = SqlScriptGenerator.getSelectCount(table);
		ResultSet rs = conn.createStatement().executeQuery(sql);
		while (rs.next()) {
			cnt = rs.getInt(1);
		}
		
		DbUtils.closeQuietly(rs);
		sql = null;
		return cnt;
	}
}
