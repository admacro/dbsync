package askul.business.quartz.dbsync;

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
	private HashMap<String, List<Column>> infoMap = null;
	private List<String> tableNamesList = null;
	private final List<Column> cols = new ArrayList<Column>();
	
	public SQLServerSide(Connection conn) {
		this.conn = conn;
	}

	/**
	 * 获取SqlServer数据库中所有表的表名
	 * 
	 * @return list 包含数据库中所有表名的list
	 * @throws SQLException
	 */
	public List<String> getTableNamesList() throws SQLException {
		return getTables();
	}
	
	private List<String> getTables() throws SQLException {
		if (tableNamesList != null && tableNamesList.size() > 0) {
			return tableNamesList;
		}
		
		String[] tableNamesArray = null;
		ResultSet rs = null;
		
		DatabaseMetaData dbmd = conn.getMetaData();
		rs = dbmd.getTables(null, null, "%", new String[] { "TABLE" });

		int size = RSHelper.getRowCount(rs);
		tableNamesArray = new String[size];
		
		int i = 0;
		while (rs.next()) {
			// 第三列为表名
			String tableName = rs.getString(3).toLowerCase();
			tableNamesArray[i++] = tableName;
		}

		DbUtils.closeQuietly(rs);
		
		tableNamesList = Arrays.asList(tableNamesArray);
		return tableNamesList;
	}
	
	public void clearTableNamesList() {
		if (this.tableNamesList != null)
			this.tableNamesList = null;
	}
	/**
	 * 根据SqlServer数据库中表的表名获取该表所有字段的信息
	 * 
	 * @param tableName
	 *            欲获取字段的表的表名
	 * @return list 包含该表所有字段信息的list
	 * @throws SQLException
	 */
	public List<Column> getColumnsByTableName(String tableName) throws SQLException {
		return getColumns(tableName);
	}
	
	private List<Column> getColumns(String tableName) throws SQLException {
		
		cols.clear();
		ResultSet rs = null;
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
		
		DbUtils.closeQuietly(rs);
		
		return cols;
	}
	
	/**
	 * 获取建表所需数据（表名及每张表对应的所有列的信息）
	 * 
	 * @return 所有建表需要的数据
	 */
	public HashMap<String, List<Column>> getInfoMap() throws SQLException {
		if (infoMap == null) {
			infoMap = new HashMap<String, List<Column>>();
			for (String table : getTables()) {
				List<Column> columns = new ArrayList<Column>();
				columns.addAll(getColumns(table));
				infoMap.put(table, columns);
			}
		}
		return infoMap;
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

	public Connection getConnection() {
		return this.conn;
	}
}
