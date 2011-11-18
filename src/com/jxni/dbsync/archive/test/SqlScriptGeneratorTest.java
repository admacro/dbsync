package com.migration.test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.migration.Column;
import com.migration.SqlScriptGenerator;

public class SqlScriptGeneratorTest extends TestCase {
	
	public void testGetCreateScript() {
		String tableName = "emp";
		
		Column c1 = new Column();
		Column c2 = new Column();
		Column c3 = new Column();
		
		c1.setName("id");
		c1.setSqlType("int");
		
		c2.setName("name");
		c2.setSqlType("nvarchar");
		c2.setLength(50);
		
		c3.setName("salary");
		c3.setSqlType("numeric");
		c3.setLength(12);
		c3.setPrecision(2);
		
		List<Column> cols = new ArrayList<Column>();
		cols.add(c1);
		cols.add(c2);
		cols.add(c3);
		
		String result = "create table emp(id integer,name character(50),salary numeric(12,2));";
		String prettyResult = "create table emp (\n\tid integer, \n\tname character(50), \n\tsalary numeric(12,2));";
		String outScript = SqlScriptGenerator.getCreateScript(tableName, cols, false);
		String outScriptFormatted = SqlScriptGenerator.getCreateScript(tableName, cols, true);
		
		assertTrue(result.equalsIgnoreCase(outScript));
		assertTrue(prettyResult.equalsIgnoreCase(outScriptFormatted));
	}
	
	public void testGetDropScript() {
		String t1 = "test1";
		String t2 = "test2";
		String t3 = "test3";
		
		List<String> tableNames = new ArrayList<String>();
		tableNames.add(t1);
		tableNames.add(t2);
		tableNames.add(t3);
		
		String result = "drop table if exists test1,test2,test3;";
		String prettyResult = "drop table if exists \n\ttest1, \n\ttest2, \n\ttest3;";
		String outScript = SqlScriptGenerator.getDropScript(tableNames, false);
		String outScriptFormatted = SqlScriptGenerator.getDropScript(tableNames, true);
		
		assertTrue(result.equalsIgnoreCase(outScript));
		assertTrue(prettyResult.equalsIgnoreCase(outScriptFormatted));
		
	}
	
	public void testGetInsertScript() {
		String tableName = "emp";
		
		List<Object> vals = new ArrayList<Object>();
		vals.add(new Integer(1));
		vals.add("jamesni");
		vals.add(new BigDecimal("123.45"));
		
		String result = "insert into emp values(1,'jamesni',123.45);";
		String prettyResult = "insert into emp values(\n\t1, \n\t'jamesni', \n\t123.45);";
		String outScript = SqlScriptGenerator.getInsertScript(tableName, vals, false);
		String outScriptFormatted = SqlScriptGenerator.getInsertScript(tableName, vals, true);
		
		assertTrue(result.equalsIgnoreCase(outScript));
		assertTrue(prettyResult.equalsIgnoreCase(outScriptFormatted));
	}
	
	public void testGetParamInsertScript() {
		String table = "emp";
		int size = 10;
		
		String result = "insert into emp values(?,?,?,?,?,?,?,?,?,?);";
		String outScript = SqlScriptGenerator.getParamInsertScript(table, size);;
		
		assertTrue(result.equalsIgnoreCase(outScript));
	}
	
	public void testGetSelectAll() {
		String table = "emp";
		String result = "select * from emp;";
		String outScript = SqlScriptGenerator.getSelectAll(table);
		assertTrue(result.equalsIgnoreCase(outScript));
	}
	
	public void testGetSelectCount() {
		String table = "emp";
		String result = "select count(*) from emp;";
		String outScript = SqlScriptGenerator.getSelectCount(table);
		assertTrue(result.equalsIgnoreCase(outScript));
	}
	
}
