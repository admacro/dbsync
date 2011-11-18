package askul.business.quartz.dbsync;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

import askul.business.common.CommonUtil;
import askul.business.mail.TemplateMail;

public class DbSyncJob implements StatefulJob {
	
	private static final Log logger = LogFactory.getLog(DbSyncJob.class);
	private static BeanFactory factory = new XmlBeanFactory(new ClassPathResource("applicationContext.xml"));
	private static final TemplateMail dbSyncMail = (TemplateMail) factory.getBean("dbSyncErrorMail");
	
	private DataSource sqlServerDs = (DataSource) factory.getBean("sqlServerDs");
	private DataSource postgresqlDs = (DataSource) factory.getBean("pgAnalyticalDs");
	
//	private DataSource sqlServerDs;
//	private DataSource postgresqlDs;
	
	private DbSyncTracker tracker = new DbSyncTracker();
	
	public void execute(JobExecutionContext arg0) 
			throws JobExecutionException {
		
		// prepare tracker and give it to haulageman
		tracker.setPostman(dbSyncMail);
		
		logger.info(">> Database synchronization start ...");
		
		boolean isSuccess = false;
		
		try {
			// haulageman :D
			DBSynchronizer haulageman = new DBSynchronizer(
					sqlServerDs.getConnection(), postgresqlDs.getConnection());
			
			// Give haulageman a tracker.
			haulageman.setTracker(tracker);
			
			// Mission, start!!! oh! oh! oh!
			isSuccess = haulageman.transmit();
			
			haulageman = null;
			
		} catch (SQLException e) {
			
			// SQLException was thrown, caused by DB connection error or sth,
			// has nothing to do with application.
			String info = ">> Oops! :( Task failed! \n"
				+ "Seems you got DB connection error or sth wrong with DB! \n"
				+ "Exception info: " 
				+ e.getMessage();
			
			logger.error(info);
			tracker.addError("main", info.replace("\n", "<br>"));
			
			isSuccess = false;
			
		}
		
		// Mission failed! Send mails to guys who should know!
		if(!isSuccess) {
			String[] addresses = 
				CommonUtil.getProperties("dbsync.error.mails").split(",");
				
				// ONLY used in test
//				TestUtils.getPropertyFromMainFilter("dbsync.error.mails").split(",");
			
			tracker.sendMail(addresses);
			logger.info("[WARNING] :p Mission completed partially! Please go to log files for details!");
		} else {
			logger.info("[SUCCESS] :D Mission completed!");
		}
		
	}

//	public DataSource getSqlServerDs() {
//		return sqlServerDs;
//	}
//
//	public void setSqlServerDs(DataSource sqlServerDs) {
//		this.sqlServerDs = sqlServerDs;
//	}
//
//	public DataSource getPostgresqlDs() {
//		return postgresqlDs;
//	}
//
//	public void setPostgresqlDs(DataSource postgresqlDs) {
//		this.postgresqlDs = postgresqlDs;
//	}

}
