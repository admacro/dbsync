package askul.business.quartz.dbsync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.microsoft.sqlserver.jdbc.SQLServerResultSet;

/**
 * 用于将MS SQL Server 2000上数据库中的数据迁移到Postgresql8.3数据库中
 * 
 * @author jamesni
 */
/**
 * @author jamesni
 *
 */
/**
 * @author jamesni
 *
 */
public class DBSynchronizer {
	
	private static final Log logger = LogFactory.getLog(DBSynchronizer.class);
	private SQLServerSide sqlServer = null;
	private PostgreSQLSide postgresql = null;
	private DbSyncTracker tracker = null;

	public DBSynchronizer(Connection sqlServerConn, Connection postgresqlConn) {
		
		sqlServer = new SQLServerSide(sqlServerConn);
		postgresql = new PostgreSQLSide(postgresqlConn);
		
	}
	
	public boolean transmit() throws SQLException {
		// Prepare for database transformation. 
		// Drop and create tables in PostgreSQL
		if (!prepare()) {
			return false;
		}
		
		// If prepared, insert data from SQLServer to PostgreSQL
		insert();
		
		// Check data integrity.
		boolean isSuccess = check();
		
		if (isSuccess) {
			logger.info(">>>>> Task completed successfully! ");
		}
		
		// Close connections to SQLServer and PostgreSQL
		finalizeJob();
		
		return isSuccess;
	}
	
	private boolean prepare() throws SQLException {
		postgresql.setTracker(tracker);
		boolean isPrepared = false;
		List<String> tables = sqlServer.getTableNamesList();
		if (postgresql.dropExistingTable(tables)) {
			Map<String,List<Column>> infoMap = sqlServer.getInfoMap();
			isPrepared = postgresql.createTable(infoMap);
		}
		return isPrepared;
	}

	private void insert() throws SQLException {
		for (String table : sqlServer.getTableNamesList()) {
			int count = sqlServer.getSelectCount(table);
			if (count == 0) continue; // skip
			if (count > 0 && count <= 500) {
				insertGeneralData(table);
			} else {
				insertMassData(table, count);
			}
		}
	}

	/**
	 * 插入数据（源表中记录条数小于等于500）
	 * 
	 * @param table 表名
	 * @throws SQLException
	 */
	private void insertGeneralData(String table) throws SQLException {
		
		logger.info(">> Inserting data into table " + table + " ...");
		
		// =========== Retrieve =========== //
		String sql = SqlScriptGenerator.getSelectAll(table);
		Statement st = sqlServer.getConnection().createStatement();
		ResultSet rs = st.executeQuery(sql);
		sql = null;
		
		List<List<Object>> data = new ArrayList<List<Object>>();
		List<Column> cols = sqlServer.getColumnsByTableName(table);
		List<Object> row = null;
		
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
		}
		
		// =========== Insert =========== //
		postgresql.getConnection().setAutoCommit(false);
		String paraSql = 
			SqlScriptGenerator.getParamInsertScript(table, cols.size());
		PreparedStatement ps = postgresql.getConnection().prepareStatement(paraSql);
		int j = 1;
		for (List<Object> r : data) {
			for (int i = 0; i < r.size(); i++) {
				ps.setObject(i + 1, r.get(i));
			}
			ps.addBatch();
			if (j % 20 == 0 || j == data.size()) {
				try {
					ps.executeBatch();
				} catch (SQLException e) {
					String info = 
						"在向表" + table +"（小表）中批量插入数据时抛出了一个SQLException异常: \n"
						+ "异常信息: " + e.getMessage();
					logger.error(info);
					tracker.addError("insert", info);
					continue;
				}
			}
			j++;
		}
		postgresql.getConnection().commit();
		
		// =========== Finalize =========== //
		DbUtils.closeQuietly(st);
		DbUtils.closeQuietly(ps);
		DbUtils.closeQuietly(rs);
		
		paraSql = null;
		data.clear(); // Clear list after commit
		data = null;
		
		logger.info(">> Inserting data into table " + table + " done!");
	}
	
	/**
	 * 插入大批量数据（count大于500），使用服务器端游标
	 * 
	 * @param table 表名
	 * @param count 源表中记录的条数
	 * @throws SQLException
	 */
	private void insertMassData(String table, int count) throws SQLException {
		
		logger.info(">> Inserting data into table " + table + " ...");
		
		// =========== Prepare for retrieving =========== //
		String sql = SqlScriptGenerator.getSelectAll(table);
		Statement st = sqlServer.getConnection().createStatement(
				SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY,
				SQLServerResultSet.CONCUR_READ_ONLY);
		ResultSet rs = st.executeQuery(sql);
		sql = null;
		
		List<Column> cols = sqlServer.getColumnsByTableName(table);
		List<List<Object>> data = new ArrayList<List<Object>>();
		PreparedStatement ps = null;
		List<Object> row = null;
		
		postgresql.getConnection().setAutoCommit(false);
		String paraSql = 
			SqlScriptGenerator.getParamInsertScript(table, cols.size());
		ps = postgresql.getConnection().prepareStatement(paraSql);
		
		int idx = 1;
		while (rs.next()) {
			// =========== Retrieve 500 rows of data =========== //
			row = new ArrayList<Object>(cols.size());
			for (int i = 0; i < cols.size(); i++) {
				if (ColumnHelper.isBinary(cols.get(i))) {
					row.add(rs.getBytes(i + 1));
				} else {
					row.add(rs.getObject(i + 1));
				}
			}
			data.add(row);
			
			// =========== Insert 500 rows of data =========== //
			if (idx % 500 == 0 || idx == count) {
				int j = 1;
				for (List<Object> r : data) {
					for (int i = 0; i < r.size(); i++) {
						ps.setObject(i + 1, r.get(i));
					}
					try {
						ps.addBatch();
						if (j % 20 == 0 || j == data.size()) {
							ps.executeBatch();
						}
					} catch (SQLException e) {
						String info = 
							"在向表" + table +"（大表）中批量插入数据时抛出了一个SQLException异常: \n"
							+ "异常信息: " + e.getMessage();
						logger.error(info);
						tracker.addError("insert", info);
						continue;
					}
					j++;
				}
				postgresql.getConnection().commit();
				data.clear(); // Clear list after commit
			}
			idx++;
		}
		
		// =========== Finalize =========== //
		DbUtils.closeQuietly(st);
		DbUtils.closeQuietly(ps);
		DbUtils.closeQuietly(rs);
		
		paraSql = null;
		data.clear();
		data = null;
		
		logger.info(">> Inserting data into table " + table + " done!");
	}

	public boolean check() throws SQLException {
		
		logger.info(">> Checking data integrity ...");
		
		int diffCount = 0;
		for (String table : sqlServer.getTableNamesList()) {
			int countOnSqlServer = sqlServer.getSelectCount(table);
			int countOnPostgresql = postgresql.getSelectCount(table);
			int diff = countOnSqlServer - countOnPostgresql;
			if (diff > 0) {
				diffCount++;
				String info = "表" + table + "中有" + diff + "条数据未同步到PostgreSQL";
				logger.info(info);
				tracker.addError("main", info);
			}
		}
		return diffCount == 0 ? true : false;
	}
	
	public void setTracker(DbSyncTracker tracker) {
		this.tracker = tracker;
	}

	public DbSyncTracker getTracker() {
		return tracker;
	}

	private void finalizeJob() {
		sqlServer.clearTableNamesList();
		DbUtils.closeQuietly(sqlServer.getConnection());
		DbUtils.closeQuietly(postgresql.getConnection());
	}
}
