package reactor.tcp.syslog.test;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import reactor.io.Buffer;
import reactor.util.Assert;

import java.nio.CharBuffer;
import java.util.Calendar;

class SyslogMessageParser {

	private static final int MAXIMUM_SEVERITY = 7;

	private static final int MAXIMUM_FACILITY = 23;

	private static final int MINIMUM_PRI = 0;

	private static final int MAXIMUM_PRI = (MAXIMUM_FACILITY * 8) + MAXIMUM_SEVERITY;

	private static final int DEFAULT_PRI = 13;

	private static final DateTimeFormatter rfc3164 =
			DateTimeFormat.forPattern("MMM d HH:mm:ss").withZoneUTC();

	enum Token {
		PRIORITY, TIMESTAMP, HOSTNAME, MESSAGE
	}

	public static void reset(CharBuffer cb) {
		cb.position(0);
		cb.limit(cb.capacity());
	}

	public static SyslogMessage parse(Buffer buffer) {
		char[] chars = new char[buffer.remaining()];
		DateTime now = DateTime.now();
		int year = now.getYear();

		Token t = Token.PRIORITY;
		int start = 0;
		int pos = 0;
		while (buffer.remaining() > 0) {
			switch ((chars[pos++] = (char) buffer.read())) {
				case '<': {
					// start priority
					start = pos;
					break;
				}
				case '>': {
					int priority = Integer.parseInt(new String(chars, start, (pos - 1) - start));
					int facility = priority / 8;
					int severity = priority % 8;
					System.out.println("pri: " + priority + ", fac: " + facility + ", sev: " + severity);

					t = Token.TIMESTAMP;
					start = pos;

					switch ((chars[pos++] = (char) buffer.read())) {
						case '-':
							// now()
							break;
						case 'J': // Jan, Jun, Jul
						case 'F': // Feb
						case 'M': // Mar, May
						case 'A': // Apr, Aug
						case 'S': // Sep
						case 'O': // Oct
						case 'N': // Nov
						case 'D': { // Dec
							// RFC3164
							Assert.state(buffer.remaining() > 15);
							for (int i = 0; i < 15; i++) {
								chars[pos++] = (char) buffer.read();
							}

							String tstamp = new String(chars, start, (pos - 1) - start);
							DateTime dt = rfc3164.parseDateTime(tstamp).withYear(year);
							System.out.println("tstamp: " + dt);

							start = pos;
							break;
						}
					}

					break;
				}
				case ' ': {
					// whitespace
					switch (t) {
						case PRIORITY:
							t = Token.TIMESTAMP;
							break;
						case TIMESTAMP:
							t = Token.HOSTNAME;

							String host = new String(chars, start, (pos - 1) - start).trim();
							System.out.println("host: " + host);

							start = pos;
							break;
						case HOSTNAME:
							t = Token.MESSAGE;
							break;
					}
					break;
				}
			}
		}

		String msg = new String(chars, start, pos - start);
		System.out.println("msg: " + msg);

		return null;
	}

	public static SyslogMessage parse(String raw) {
		if (null == raw || raw.isEmpty()) {
			return null;
		}

		int pri = DEFAULT_PRI;
		Timestamp timestamp = null;
		String host = null;

		int index = 0;

		if (raw.indexOf('<') == 0) {
			int endPri = raw.indexOf('>');
			if (endPri >= 2 && endPri <= 4) {
				int candidatePri = Integer.parseInt(raw.substring(1, endPri));
				if (candidatePri >= MINIMUM_PRI && candidatePri <= MAXIMUM_PRI) {
					pri = candidatePri;
					index = endPri + 1;
					timestamp = new Timestamp(raw.substring(index, index + 15));
					index += 16;
					int endHostnameIndex = raw.indexOf(' ', index);
					host = raw.substring(index, endHostnameIndex);
					index = endHostnameIndex + 1;
				}
			}
		}

		if (host == null) {
			// TODO Need a way to populate host with client's IP address
		}

		int facility = pri / 8;
		Severity severity = Severity.values()[pri % 8];

		if (timestamp == null) {
			timestamp = new Timestamp(Calendar.getInstance());
		}

		return new SyslogMessage(facility, severity, timestamp, host, raw.substring(index));
	}

}
