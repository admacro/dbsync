/**
 * Askul Business System
 * Developed By Netage Technology (Shanghai) Inc.
 * CreateDate: Jan 13, 2009
 */
package askul.business.quartz.dbsync;

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

	private static StringBuilder scriptBuilder = new StringBuilder(100);
	
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
		
		clearScriptBuilder();
		
		scriptBuilder
				.append(CREATE_PREFIX).append(BLANK_SPACE)
				.append(ColumnHelper.quoteColumnName(tableName)).append(OPEN_BRACKET);
		
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
		
		scriptBuilder.trimToSize();
		return scriptBuilder.toString();
	}

	private static String getPrettyCreateScript(
			String tableName, List<Column> cols) {
		
		clearScriptBuilder();
		
		scriptBuilder
				.append(CREATE_PREFIX).append(BLANK_SPACE)
				.append(ColumnHelper.quoteColumnName(tableName)).append(BLANK_SPACE)
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
		
		scriptBuilder.trimToSize();
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
		
		clearScriptBuilder();
		
		scriptBuilder.append(DROP_PREFIX).append(BLANK_SPACE);
		
		int i = 0;
		for (String tableName : tableNames) {
			scriptBuilder.append(ColumnHelper.quoteColumnName(tableName));
			if (i++ != tableNames.size() - 1) {
				scriptBuilder.append(COLUMN_SEPARATOR);
			} else {
				scriptBuilder.append(SQL_SUFFIX);
			}
		}
		
		scriptBuilder.trimToSize();
		return scriptBuilder.toString();
		
	}

	private static String getPrettyDropScript(List<String> tableNames) {
		
		clearScriptBuilder();
		
		scriptBuilder.append(DROP_PREFIX).append(BLANK_SPACE).append(NEW_LINE);
		
		int i = 0;
		for (String tableName : tableNames) {
			scriptBuilder.append(TAB).append(ColumnHelper.quoteColumnName(tableName));
			if (i++ != tableNames.size() - 1) {
				scriptBuilder
						.append(COLUMN_SEPARATOR)
						.append(BLANK_SPACE)
						.append(NEW_LINE);
			} else {
				scriptBuilder.append(SQL_SUFFIX);
			}
		}
		
		scriptBuilder.trimToSize();
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
		
		clearScriptBuilder();
		
		scriptBuilder
				.append(INSERT_PREFIX).append(BLANK_SPACE)
				.append(ColumnHelper.quoteColumnName(table)).append(BLANK_SPACE)
				.append(INSERT_INFIX).append(OPEN_BRACKET);
		
		for (int i = 0; i < size; i++) {
			scriptBuilder.append(PARAM);
			
			if (i != size - 1) {
				scriptBuilder.append(COLUMN_SEPARATOR);
			} else {
				scriptBuilder.append(CLOSE_BRACKET).append(SQL_SUFFIX);
			}
			
		}
		
		scriptBuilder.trimToSize();
		return scriptBuilder.toString();
		
	}
	
	/**
	 * Clear characters in the scriptbuilder.
	 * 
	 * @param scriptBuilder StringBuilder need to be clear up
	 */
	private static void clearScriptBuilder() {
		if (scriptBuilder.length() > 0) {
			scriptBuilder.delete(0, scriptBuilder.length());
		}
	}
	
}
