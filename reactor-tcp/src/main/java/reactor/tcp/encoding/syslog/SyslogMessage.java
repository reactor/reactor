package reactor.tcp.encoding.syslog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Jon Brisbin
 */
public class SyslogMessage {

	private static final int    MAXIMUM_SEVERITY = 7;
	private static final int    MAXIMUM_FACILITY = 23;
	private static final int    MINIMUM_PRI      = 0;
	private static final int    MAXIMUM_PRI      = (MAXIMUM_FACILITY * 8) + MAXIMUM_SEVERITY;
	private static final int    DEFAULT_PRI      = 13;
	private static final String RFC3414          = "MMM d HH:mm:ss";

	private final String raw;
	private final int    priStart;
	private final int    priEnd;
	private final int    tstampStart;
	private final int    tstampEnd;
	private final int    hostStart;
	private final int    hostEnd;
	private final int    msgStart;
	private Integer priority  = null;
	private Integer facility  = null;
	private Integer severity  = null;
	private Date    timestamp = null;
	private String  host      = null;
	private String  message   = null;

	public SyslogMessage(String raw,
											 int priStart, int priEnd,
											 int tstampStart, int tstampEnd,
											 int hostStart, int hostEnd,
											 int msgStart) {
		this.raw = raw;
		this.priStart = priStart;
		this.priEnd = priEnd;
		this.tstampStart = tstampStart;
		this.tstampEnd = tstampEnd;
		this.hostStart = hostStart;
		this.hostEnd = hostEnd;
		this.msgStart = msgStart;
	}

	public int getPriority() {
		if (null == priority) {
			if (priStart > 0) {
				int pri = Integer.valueOf(raw.substring(priStart, priEnd));
				if (pri >= MINIMUM_PRI && pri <= MAXIMUM_PRI) {
					priority = pri;
					facility = pri / 8;
					severity = pri % 8;
				}
			} else {
				priority = DEFAULT_PRI;
				facility = priority / 8;
				severity = priority % 8;
			}
		}
		return priority;
	}

	public int getFacility() {
		getPriority();
		return facility;
	}

	public int getSeverity() {
		getPriority();
		return severity;
	}

	public Date getTimestamp() {
		if (null == timestamp) {
			if (tstampStart >= 0) {
				SimpleDateFormat sdf = new SimpleDateFormat(RFC3414);
				try {
					timestamp = sdf.parse(raw.substring(tstampStart, tstampEnd));
				} catch (ParseException e) {
					timestamp = new Date();
				}
			} else {
				timestamp = new Date();
			}
		}
		return timestamp;
	}

	public String getHost() {
		if (null == host) {
			if (hostStart > 0) {
				host = raw.substring(hostStart, hostEnd);
			} else {
				host = "";
			}
		}
		return host;
	}

	public String getMessage() {
		if (null == message) {
			message = raw.substring(msgStart);
		}
		return message;
	}

}
