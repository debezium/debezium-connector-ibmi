package com.ibm.as400.access;

import java.lang.StackWalker.StackFrame;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AS400JDBCConnectionForcedCcsid extends AS400JDBCConnectionImpl {
	private static Logger log = Logger.getLogger(AS400JDBCConnectionForcedCcsid.class.toString());

	public static final String FROM_CCSID = "from.ccsid";
	public static final String TO_CCSID = "to.ccsid";
	private Integer fromCcsid;
	private Integer toCcsid;

	public AS400JDBCConnectionForcedCcsid() {
		super();
	}

	@Override
	public ConvTable getConverter(int ccsid) throws SQLException {
		if (this.fromCcsid != null && fromCcsid.intValue() == ccsid && toCcsid != null) {
			log.fine(() -> String.format("requested ccsid %d using replacement ccsid %d\n\t%s", ccsid, toCcsid,
					getStack()));
			return super.getConverter(this.toCcsid);
		}
		log.fine(() -> String.format("requested ccsid %d using parent converter\n\t%s", ccsid, getStack()));
		return super.getConverter(ccsid);
	}

	@Override
	public void setProperties(JDDataSourceURL dataSourceUrl, JDProperties properties, AS400 as400, Properties info)
			throws SQLException {
		super.setProperties(dataSourceUrl, properties, as400, info);
		fromCcsid = getInteger(properties, info, FROM_CCSID);
		toCcsid = getInteger(properties, info, TO_CCSID);
	}
	
	public Integer getInteger(JDProperties urlProperties, Properties info, String key) {
		final int index = JDProperties.getPropertyIndex(key);
		if (index > -1) {
			return urlProperties.getInt(index);
		}
		final String jv = info.getProperty(key);
		if (jv != null) {
			return Integer.parseInt(jv);
		}
		return null;
	}

	public List<String> toString(Stream<StackFrame> stackFrameStream) {

		return stackFrameStream
				.map(f -> String.format("%s.%s#%d", f.getClassName(), f.getMethodName(), f.getLineNumber()))
				.collect(Collectors.toList());
	}

	private String getStack() {
		final List<String> stackTrace = StackWalker.getInstance().walk(this::toString);
		return String.join("\n\t", stackTrace);
	}
}
