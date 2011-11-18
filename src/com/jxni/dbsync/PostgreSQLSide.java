package askul.business.quartz.dbsync;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PostgreSQLSide {
	
	private static final Log logger = LogFactory.getLog(PostgreSQLSide.class);
	private final Connection conn;
	private DbSyncTracker tracker;
	
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
		
		logger.info(">> Dropping tables in PostgreSQL ...");
		
		boolean isSuccess = true;
		String dropScript = SqlScriptGenerator.getDropScript(tableNames, false);
		Statement st = conn.createStatement();
		
		try {
			st.executeUpdate(dropScript);
		} catch (SQLException e) {
			String info = "在执行下面的删表SQL语句时抛出了一个SQLException异常: \n"
				+ e.getMessage() + "\n"
				+ SqlScriptGenerator.getDropScript(tableNames, true);
			logger.error(info);
			tracker.addError("drop", info.replace("\n", "<br>"));
			// Task fails if any excetpion is thrown.
			isSuccess = false;
		} finally {
			dropScript = null;
			DbUtils.closeQuietly(st);
		}
		
		if (isSuccess) {
			logger.info(">> All tables in PostgreSQL dropped!");
		} else {
			logger.info(">> Mission crashed at dropping tables!");
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
		
		logger.info(">> Creating tables in PostgreSQL ...");
		
		Statement st = null;
		String sql = null;
		boolean isSuccess = true;
		
		st = conn.createStatement();	
		
		Iterator<String> iter = infoMap.keySet().iterator();
		
		while(iter.hasNext()) {
			String table = iter.next();
			List<Column> cols = infoMap.get(table);
			sql = SqlScriptGenerator.getCreateScript(table, cols, false);
			
			try {
				st.executeUpdate(sql);
			} catch (SQLException se) {
				String info = "在执行下面的建表SQL语句时抛出了一个SQLException异常: \n"
					+ se.getMessage() + "\n"
					+ SqlScriptGenerator.getCreateScript(table, cols, true);
				logger.error(info);
				tracker.addError("create", info.replace("\n", "<br>"));
				// Task fails if any excetpion is thrown.
				isSuccess = false;
			}
			sql = null;
		}
		
		if (isSuccess) {
			logger.info(">> All tables in PostgreSQL created!");
		} else {
			logger.info(">> Mission crashed at creating tables!");
		}
		
		DbUtils.closeQuietly(st);
		
		return isSuccess;
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
	
	public DbSyncTracker getTracker() {
		return tracker;
	}

	public void setTracker(DbSyncTracker tracker) {
		this.tracker = tracker;
	}
	
	public Connection getConnection() {
		return this.conn;
	}
}
