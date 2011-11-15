/**
 * Askul Business System
 * Developed By Netage Technology (Shanghai) Inc.
 * CreateDate: Jan 13, 2009
 */
package com.migration;

import java.util.List;

/**
 * Generate sql scripts for table dropping, creation and data insertion.
 * 
 * @author jamesni
 */
public class SqlScriptGenerator {

	private final static String CREATE_PREFIX = "CREATE TABLE";
	private final static String DROP_PREFIX = "DROP TABLE IF EXISTS";
	private final static String INSERT_PREFIX = "INSERT INTO";
	private final static String INSERT_INFIX = "VALUES";
	private final static String SELECT_COUNT = "SELECT COUNT(*) FROM";
	private static final String SELECT_ALL = "SELECT * FROM";
	private final static char SQL_SUFFIX = ';';
	private final static char COLUMN_SEPARATOR = ',';
	private final static char OPEN_BRACKET = '(';
	private final static char CLOSE_BRACKET = ')';
	private final static char BLANK_SPACE = ' ';
	private final static char PARAM = '?';
	private final static String NEW_LINE = "\n";
	private final static String TAB = "\t";

	private static StringBuilder scriptBuilder = new StringBuilder();

	/**
	 * Generate sql script for table creation.
	 * 
	 * @param tableName name of the table to create
	 * @param cols holds columns' info
	 * @param format whether format script or not
	 * @return generated script (can be execute directly)
	 */
	public static String getCreateScript(
			String tableName, List<Column> cols, boolean format) {
		if (format) {
			return getPrettyCreateScript(tableName, cols);
		} else {
			return getCreateScript(tableName, cols);
		}
	}

	private static String getCreateScript(String tableName, List<Column> cols) {
		
		scriptBuilder.delete(0, scriptBuilder.length());
		scriptBuilder
				.append(CREATE_PREFIX).append(BLANK_SPACE)
				.append(tableName).append(OPEN_BRACKET);
		
		int i = 0;
		for (Column c : cols) {
			scriptBuilder
					.append(c.getName())
		            .append(BLANK_SPACE)
		            .append(ColumnHelper.convert(c));
			if (i++ == cols.size() - 1) {
				scriptBuilder.append(CLOSE_BRACKET).append(SQL_SUFFIX);
			} else {
				scriptBuilder.append(COLUMN_SEPARATOR);
			}
		}
		
		return scriptBuilder.toString();
	}

	private static String getPrettyCreateScript(
			String tableName, List<Column> cols) {
		
		scriptBuilder.delete(0, scriptBuilder.length());
		scriptBuilder
				.append(CREATE_PREFIX).append(BLANK_SPACE)
				.append(tableName).append(BLANK_SPACE)
				.append(OPEN_BRACKET).append(NEW_LINE);
		
		int i = 0;
		for (Column c : cols) {
			scriptBuilder
					.append(TAB)
					.append(c.getName())
					.append(BLANK_SPACE)
					.append(ColumnHelper.convert(c));
			if (i++ == cols.size() - 1) {
				scriptBuilder
						.append(CLOSE_BRACKET)
						.append(SQL_SUFFIX);
			} else {
				scriptBuilder
						.append(COLUMN_SEPARATOR)
						.append(BLANK_SPACE)
						.append(NEW_LINE);
			}
		}
		
		return scriptBuilder.toString();
	}

	/**
	 * Generate sql script for table dropping.
	 * 
	 * @param tableNames names of the tables to drop
	 * @param format whether format script or not
	 * @return generated script (can be execute directly)
	 */
	public static String getDropScript(List<String> tableNames, boolean format) {
		
		if (format) {
			return getPrettyDropScript(tableNames);
		} else {
			return getDropScript(tableNames);
		}

	}

	private static String getDropScript(List<String> tableNames) {
		
		scriptBuilder.delete(0, scriptBuilder.length());
		scriptBuilder.append(DROP_PREFIX).append(BLANK_SPACE);
		
		int i = 0;
		for (String tableName : tableNames) {
			scriptBuilder.append(tableName);
			if (i++ != tableNames.size() - 1) {
				scriptBuilder.append(COLUMN_SEPARATOR);
			} else {
				scriptBuilder.append(SQL_SUFFIX);
			}
		}
		
		return scriptBuilder.toString();
		
	}

	private static String getPrettyDropScript(List<String> tableNames) {
		
		scriptBuilder.delete(0, scriptBuilder.length());
		scriptBuilder.append(DROP_PREFIX).append(BLANK_SPACE).append(NEW_LINE);
		
		int i = 0;
		for (String tableName : tableNames) {
			scriptBuilder.append(TAB).append(tableName);
			if (i++ != tableNames.size() - 1) {
				scriptBuilder
						.append(COLUMN_SEPARATOR)
						.append(BLANK_SPACE)
						.append(NEW_LINE);
			} else {
				scriptBuilder.append(SQL_SUFFIX);
			}
		}
		
		return scriptBuilder.toString();
		
	}

	/**
	 * Generate sql script for data insertion.
	 * <br>
	 * <b>CAUTION</b><br>
	 * *ONLY* use this when there is no binary type column in the table.
	 * <br>
	 * <b>binary type</b> refers to: image, binary and varbinary
	 * 
	 * @param tableName name of the table to insert data into
	 * @param cols table columns list
	 * @param vals datas to insert
	 * @param format whether format script or not
	 * @return generated script (can be execute directly)
	 */
	public static String getInsertScript(
			String tableName, List<Object> vals, boolean format) {

		if (format) {
			return getPrettyInsertScript(tableName, vals);
		} else {
			return getInsertScript(tableName, vals);
		}
		
	}

	private static String getInsertScript(String tableName, List<Object> vals) {
		
		scriptBuilder.delete(0, scriptBuilder.length());
		scriptBuilder
				.append(INSERT_PREFIX).append(BLANK_SPACE)
				.append(tableName).append(BLANK_SPACE)
				.append(INSERT_INFIX).append(OPEN_BRACKET);
		
		int i = 0;
		for (Object v : vals) {
			scriptBuilder.append(ColumnHelper.getValueByType(v));
			
			if (i++ != vals.size() - 1) {
				scriptBuilder.append(COLUMN_SEPARATOR);
			} else {
				scriptBuilder.append(CLOSE_BRACKET).append(SQL_SUFFIX);
			}
		}
		
		return scriptBuilder.toString();
		
	}

	private static String getPrettyInsertScript(String tableName, List<Object> vals) {
		
		scriptBuilder.delete(0, scriptBuilder.length());
		scriptBuilder
				.append(INSERT_PREFIX).append(BLANK_SPACE)
				.append(tableName).append(BLANK_SPACE)
				.append(INSERT_INFIX).append(OPEN_BRACKET)
				.append(NEW_LINE);
		
		int i = 0;
		for (Object v : vals) {
			scriptBuilder.append(TAB).append(ColumnHelper.getValueByType(v));
			
			if (i++ != vals.size() - 1) {
				scriptBuilder
						.append(COLUMN_SEPARATOR)
						.append(BLANK_SPACE)
						.append(NEW_LINE);
			} else {
				scriptBuilder.append(CLOSE_BRACKET).append(SQL_SUFFIX);
			}
		}
		
		return scriptBuilder.toString();
		
	}
	
	/**
	 * 生成返回表中记录数的sql语句。<br>
	 * 例：select count(*) from tableName;
	 * 
	 * @param table 表名
	 * @return 用于表中记录数的sql语句
	 */
	public static String getSelectCount(String table) {
		return SELECT_COUNT + BLANK_SPACE + table + SQL_SUFFIX;
	}
	
	/**
	 * 生成返回所有行的sql语句。<br>
	 * 例：select * from tableName
	 * 
	 * @param table 表名
	 * @return 用于返回所有行的sql语句
	 */
	public static String getSelectAll(String table) {
		return SELECT_ALL + BLANK_SPACE + table + SQL_SUFFIX;
	}

	/**
	 * 生成参数化的插入语句
	 * 
	 * @param table 表名
	 * @param size 参数的个数（即表的列数）
	 * @return 参数化了的sql插入脚本
	 */
	public static String getParamInsertScript(String table, int size) {
		
		scriptBuilder.delete(0, scriptBuilder.length());
		scriptBuilder
				.append(INSERT_PREFIX).append(BLANK_SPACE)
				.append(table).append(BLANK_SPACE)
				.append(INSERT_INFIX).append(OPEN_BRACKET);
		
		for (int i = 0; i < size; i++) {
			scriptBuilder.append(PARAM);
			
			if (i != size - 1) {
				scriptBuilder.append(COLUMN_SEPARATOR);
			} else {
				scriptBuilder.append(CLOSE_BRACKET).append(SQL_SUFFIX);
			}
			
		}
		
		return scriptBuilder.toString();
		
	}
	
}
