package reactor.tcp.syslog.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SyslogMessage {


	private static final Pattern TAG_AND_CONTENT_PATTERN = Pattern.compile("([A-Za-z0-9]+)(.*)");

	private final int facility;

	private final Severity severity;

	private final Timestamp timestamp;

	private final String host;

	private final String message;

	SyslogMessage(int facility, Severity severity, Timestamp timestamp, String host, String message) {
		this.facility = facility;
		this.severity = severity;
		this.timestamp = timestamp;
		this.host = host;
		this.message = message;
	}

	public int getFacility() {
		return facility;
	}

	public Severity getSeverity() {
		return severity;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public String getHost() {
		return host;
	}

	public String getMessage() {
		return message;
	}

	public String getTag() {
		Matcher matcher = TAG_AND_CONTENT_PATTERN.matcher(message);
		if (matcher.matches()) {
			return matcher.group(1);
		} else {
			return null;
		}
	}

	public String getContent() {
		Matcher matcher = TAG_AND_CONTENT_PATTERN.matcher(message);
		if (matcher.matches()) {
			return matcher.group(2);
		} else {
			return null;
		}
	}

	@Override
	public String toString() {
		return "SyslogMessage [facility=" + facility + ", severity=" + severity + ", timestamp=" + timestamp + ", host=" + host + ", message=" + message + "]";
	}
}