package reactor.tcp.encoding.syslog;

import java.util.Date;

/**
 * @author Jon Brisbin
 */
public class SyslogMessage {

	private final String raw;
	private final int    priority;
	private final int    facility;
	private final int    severity;
	private final Date   timestamp;
	private final String host;
	private final String message;

	public SyslogMessage(String raw,
											 int priority,
											 int facility,
											 int severity,
											 Date timestamp,
											 String host,
											 String message) {
		this.raw = raw;
		this.priority = priority;
		this.facility = facility;
		this.severity = severity;
		this.timestamp = timestamp;
		this.host = host;
		this.message = message;
	}

	public int getPriority() {
		return priority;
	}

	public int getFacility() {
		return facility;
	}

	public int getSeverity() {
		return severity;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public String getHost() {
		return host;
	}

	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return raw;
	}

}
