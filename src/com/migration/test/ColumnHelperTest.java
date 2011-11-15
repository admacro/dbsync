package com.migration.test;

import java.sql.Timestamp;
import java.util.Date;

import junit.framework.TestCase;

import com.migration.Column;
import com.migration.ColumnHelper;

public class ColumnHelperTest extends TestCase {
	
	public void testConvert() {
		String[] sqlServerTypes = new String[]{
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
		};
		
		Column[] cols = new Column[sqlServerTypes.length];
		
		for (int i = 0; i < sqlServerTypes.length; i++) {
			Column c = new Column();
			c.setSqlType(sqlServerTypes[i]);
			c.setLength(10);
			c.setPrecision(3);
			cols[i] = c;
		}
		
		String[] results = new String[] {
				"bigint", // convert mode --> 0
				"bigint", // convert mode --> 0
				"bytea", // convert mode --> 0
				"boolean", // convert mode --> 0
				"character(10)", // convert mode --> 1
				"timestamp without time zone", // convert mode --> 0
				"numeric(10,3)", // convert mode --> 2
				"numeric(10,3)", // convert mode --> 2
				"real", // convert mode --> 0
				"bytea", // convert mode --> 0
				"integer", // convert mode --> 0
				"integer", // convert mode --> 0
				"numeric(10,3)", // convert mode --> 2
				"character(10)", // convert mode --> 1
				"text", // convert mode --> 0
				"numeric(10,3)", // convert mode --> 2
				"numeric(10,3)", // convert mode --> 2
				"character(10)", // convert mode --> 1
				"real", // convert mode --> 0
				"timestamp without time zone", // convert mode --> 0
				"smallint", // convert mode --> 0
				"smallint", // convert mode --> 0
				"numeric(10,3)", // convert mode --> 2
				"character varying(10)", // convert mode --> 1
				"text", // convert mode --> 0
				"character varying(10)", // convert mode --> 1
				"integer", // convert mode --> 0
				"integer", // convert mode --> 0
				"character varying(10)", // convert mode --> 1
				"bytea", // convert mode --> 0
				"character varying(10)", // convert mode --> 1
		};
		
		for (int i = 0; i < results.length; i++) {
			assertEquals(results[i], ColumnHelper.convert(cols[i]));
		}
		
	}

	public void testGetValueByType() {
		
		long time = System.currentTimeMillis();
		
		String s = "String object";
		Date d = new Date(time);
		Timestamp t = new Timestamp(time);
		Boolean bTrue = new Boolean(true);
		Boolean bFalse = new Boolean(false);
		
		String resultS = "'" + s + "'";
		String resultD = "'" + d.toString() + "'";
		String resultT = "'" + t.toString() + "'";
		String reslutBTrue = "'1'";
		String reslutBFalse = "'0'";
		
		assertEquals(resultS, ColumnHelper.getValueByType(s));
		assertEquals(resultD, ColumnHelper.getValueByType(d));
		assertEquals(resultT, ColumnHelper.getValueByType(t));
		assertEquals(reslutBTrue, ColumnHelper.getValueByType(bTrue));
		assertEquals(reslutBFalse, ColumnHelper.getValueByType(bFalse));
		
	}

	public void testQuoteColumnName() {
		
		String name1 = "FirstName";
		String name2 = "OffSet";
		String name3 = "Sysid";
		
		String result1 = "\"firstname\"";
		String result2 = "\"offset\"";
		String result3 = "\"sysid\"";
		
		assertEquals(result1, ColumnHelper.quoteColumnName(name1));
		assertEquals(result2, ColumnHelper.quoteColumnName(name2));
		assertEquals(result3, ColumnHelper.quoteColumnName(name3));
		
	}
	
	public void testIsBinary() {
		
		Column c1 = new Column();
		Column c2 = new Column();
		Column c3 = new Column();
		Column c4 = new Column();
		
		c1.setSqlType("image");
		c2.setSqlType("binary");
		c3.setSqlType("varbinary");
		c4.setSqlType("character");
		
		assertTrue(ColumnHelper.isBinary(c1));
		assertTrue(ColumnHelper.isBinary(c2));
		assertTrue(ColumnHelper.isBinary(c3));
		assertFalse(ColumnHelper.isBinary(c4));
		
	}
}
