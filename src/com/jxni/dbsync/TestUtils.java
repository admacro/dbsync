package askul.business.quartz.dbsync;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author 高仕才
 */
public class TestUtils {

    /**
     * Log instance for this class.
     */
    public static final Log log = LogFactory.getLog(TestUtils.class);

    private static Properties p = null;

    static {
        loadPropertiesFile("src/main/filters/mainFilter.properties");
    }

    /**
     * load Properties file
     *
     * @param fileName this is a file name
     * @author 高仕才
     */
    private static void loadPropertiesFile(String fileName) {
        p = new Properties();
        InputStream is;
        try {
            is = new BufferedInputStream(new FileInputStream(fileName));
            p.load(is);
        } catch (Exception e) {
            log.info("TestUtils :" + e.getMessage());
        }
    }

    /**
     * get dataSource
     *
     * @return DataSource
     * @throws ClassNotFoundException this is a exception
     * @author 高仕才 nixiaoqing
     */
    public static DataSource getDataSource(String ds) {
        String driver = p.getProperty(ds + ".jdbc.driverClassName");
        String url = p.getProperty(ds + ".jdbc.url");
        String username = p.getProperty(ds + ".jdbc.username");
        String password = p.getProperty(ds + ".jdbc.password");

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return new DriverManagerDataSource(url, username, password);
    }

    /**
     * get dataSource
     *
     * @param key file's key
     * @return String
     * @author nixiaoqing
     */
    public static String getPropertyFromMainFilter(String key) {
		return p.getProperty(key);
	}
}
