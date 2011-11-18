package askul.business.quartz.dbsync;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RSHelper {
	
	/**
	 * 获取结果集的总行数，不改变游标的位置(还是为0)
	 * 
	 * @param rs
	 *            欲获取行数的ResultSet
	 * @return 结果集rs的总行数
	 * @throws SQLException
	 */
	public static int getRowCount(ResultSet rs) throws SQLException {
		int count = 0;
		if (rs.last()) {
			count = rs.getRow();
			rs.beforeFirst(); // 游标回滚到初始位置(0)
		}
		return count;
	}
}
