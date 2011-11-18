package com.migration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * 用于将MS SQL Server 2000上WeWay_JXC36数据库中的数据迁移到Postgresql8.3数据库中
 * 
 * @author jamesni
 */
public class DBSynchronizer {
	
	private Connection connSqlServer = ConnectionProvider.getSqlServerConnection();
	private Connection connPgSql = ConnectionProvider.getPgSqlConnection();
	
	private SQLServerSide sqlServer = new SQLServerSide(connSqlServer);
	private PostgreSQLSide postgresql = new PostgreSQLSide(connPgSql);

	public DBSynchronizer() {
	}
	
	private boolean prepare() throws SQLException {
		if (postgresql.dropExistingTable(sqlServer.getTableNamesList())) {
			return postgresql.createTable(sqlServer.getInfoMap());
		}
		return false;
	}
	
	public boolean transmitData() throws SQLException {
		
		int i = 0;
		if (!prepare()) {
			return false;
		}
		
		for (String table : sqlServer.getTableNamesList()) {
			if (sqlServer.isEmpty(table)) {
				continue;
			}
			
			if (!sqlServer.containsBinCol(table)) {
				List<String> scripts = sqlServer.getInsertScripts(table);
				if (!postgresql.insertData(scripts)) {
					i++;
				}
				scripts = null;
			} else {
				List<List<Object>> data = sqlServer.getDataWithBin(table);
				if (!postgresql.insertDataWithBin(table, data)) {
					i++;
				}
				data = null;
			}
			
		}
		
		return i > 0 ? false : true;
		
	}

}
