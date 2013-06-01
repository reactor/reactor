package reactor.tcp.encoding.syslog;

import reactor.fn.tuples.Tuple6;

/**
 * @author Jon Brisbin
 */
public class SyslogMessage extends Tuple6<Integer, Integer, Integer, String, String, String> {

	public SyslogMessage(int priority,
											 int facility,
											 int severity,
											 String timestamp,
											 String host,
											 String msg) {
		super(priority, facility, severity, timestamp, host, msg);
	}

	public int getPriority() {
		return getT1();
	}

	public int getFacility() {
		return getT2();
	}

	public int getSeverity() {
		return getT3();
	}

	public String getTimestamp() {
		return getT4();
	}

	public String getHostname() {
		return getT5();
	}

	public String getMessage() {
		return getT6();
	}

}
