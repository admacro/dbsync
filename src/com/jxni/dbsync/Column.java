package askul.business.quartz.dbsync;

/**
 * Contains some metadata of a column
 * 
 * @author nixiaoqing
 * 
 */
public class Column {

	private int length;
	private int precision;
	private String name;
	private String sqlType;

	public Column() {
	}

	public Column(String columnName) {
		setName(columnName);
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSqlType() {
		return sqlType;
	}

	public void setSqlType(String sqlType) {
		this.sqlType = sqlType;
	}

}
