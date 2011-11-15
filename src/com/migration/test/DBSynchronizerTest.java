package com.migration.test;

import java.sql.SQLException;

import junit.framework.TestCase;

import com.migration.DBSynchronizer;

public class DBSynchronizerTest extends TestCase {
	private DBSynchronizer dbs = null;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		dbs = new DBSynchronizer();
	}

	public void testTransmitData() throws SQLException {
		assertTrue(dbs.transmitData());
	}
	
}
