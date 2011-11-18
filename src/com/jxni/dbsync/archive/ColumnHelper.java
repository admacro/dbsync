package com.migration;

import java.sql.Timestamp;
import java.util.Date;

/**
 * @author nixiaoqing
 *
 */
public class ColumnHelper {
	
	/** Data type in Microsoft SQL Server 2000 */
	private static String[] sqlServerTypes = new String[] {
			"bigint", // 0
			"bigint identity", // 1
			"binary", // 2
			"bit", // 3
			"char", // 4
			"datetime", // 5
			"decimal", // 6
			"decimal() identity", // 7
			"float", // 8
			"image", // 9
			"int", // 10
			"int identity", // 11
			"money", // 12
			"nchar", // 13
			"ntext", // 14
			"numeric", // 15
			"numeric() identity", // 16
			"nvarchar", // 17
			"real", // 18
			"smalldatetime", // 19
			"smallint", // 20
			"smallint identity", // 21
			"smallmoney", // 22
			"sysname", // 23
			"text", // 24
			"timestamp", // 25
			"tinyint", // 26
			"tinyint identity", // 27
			"uniqueidentifier", // 28
			"varbinary", // 29
			"varchar" // 30
			// 总数31
	};

	/** Data type in Postgresql 8.3 */
	private static String[] postgresqlTypes = new String[]{
			"bigint", // convert mode --> 0
			"bigint", // convert mode --> 0
			"bytea", // convert mode --> 0
			"boolean", // convert mode --> 0
			"character", // convert mode --> 1
			"timestamp without time zone", // convert mode --> 0
			"numeric", // convert mode --> 2
			"numeric", // convert mode --> 2
			"real", // convert mode --> 0
			"bytea", // convert mode --> 0
			"integer", // convert mode --> 0
			"integer", // convert mode --> 0
			"numeric", // convert mode --> 2
			"character", // convert mode --> 1
			"text", // convert mode --> 0
			"numeric", // convert mode --> 2
			"numeric", // convert mode --> 2
			"character", // convert mode --> 1
			"real", // convert mode --> 0
			"timestamp without time zone", // convert mode --> 0
			"smallint", // convert mode --> 0
			"smallint", // convert mode --> 0
			"numeric", // convert mode --> 2
			"character varying", // convert mode --> 1
			"text", // convert mode --> 0
			"character varying", // convert mode --> 1
			"integer", // convert mode --> 0
			"integer", // convert mode --> 0
			"character varying", // convert mode --> 1
			"bytea", // convert mode --> 0
			"character varying", // convert mode --> 1
			// 总数31
		};
	
	/**
	 * 0 -> no length, no precision 
	 * 1 -> length, no precision 
	 * 2 -> length and precision
	 */
	private static Integer[] generalDataTypes = new Integer[] { 
		0, 0, 0, 0, 1, 0, 2, 2, 0, 0, 0, 0, 2, 1, 0, 2, 
		2, 1, 0, 0, 0, 0, 2, 1, 0, 1, 0, 0, 1, 0, 1
	};
	
	public static String convert(Column c) {
		String typeName = c.getSqlType();
		int length = c.getLength();
		int precision = c.getPrecision();
		
		String s = "";
		int idx = -1;
		for(int i = 0; i < sqlServerTypes.length; i++) {
			if(sqlServerTypes[i].equalsIgnoreCase(typeName)) {
				idx = i;
				break;
			} 
		}
		if (idx != -1) {
			switch(generalDataTypes[idx]) {
			case 0:
				s = postgresqlTypes[idx];
				break;
			case 1:
				s = length != 0 ? 
						postgresqlTypes[idx] + '(' + length + ')' : 
						postgresqlTypes[idx] + "(20)";
				break;
			case 2:
				s = (length != 0 
						&& String.valueOf(length).length() != 0 
						&& String.valueOf(precision).length() != 0) ? 
						postgresqlTypes[idx] + '(' + length + ',' + precision + ')' : 
						postgresqlTypes[idx] + "(18,4)";
				break;
			}
		} else {
			// Data types which cannot be recongnized are all set to be
			// their original type.
			s = typeName;
		}
		return s;
	}
	
	/**
	 * 根据值的对象类型将其转化成可用于插入数据库的对象值
	 * 
	 * @param value 需要转化的值对象
	 * @return 转化后的值对象
	 */
	public static Object getValueByType(Object value) {
		if (value != null) {
			if (value instanceof String 
					|| value instanceof Date 
					|| value instanceof Timestamp) {
				value = "'" + value + "'";
			} else if (value instanceof Boolean) {
				value = ((Boolean) value == true) ? "'1'" : "'0'";
			}
		}
		return value;
	}
	
	/**
	 * 将字段名全部小写并在两边加上引号
	 * 
	 * @param name
	 * @return
	 */
	public static String quoteColumnName(String name) {
		return '"' + name.toLowerCase() + '"';
	}
	
	/**
	 * 判断该字段的数据类型是否是二进制的(binary, image 或 varbinary)
	 * 
	 * @param c 传入的字段对象
	 * @return true 字段数据类型为二进制类型，false 为基本类型
	 */
	public static boolean isBinary(Column c) {
		String type = c.getSqlType();
		if ("binary".equalsIgnoreCase(type)
				|| "image".equalsIgnoreCase(type) 
				|| "varbinary".equalsIgnoreCase(type)) {
			return true;
		}
		return false;
	}
}
