package reactor.tcp.encoding.syslog;

import reactor.io.Buffer;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class SyslogParser {

	private static final int BUFFER_SIZE = 16 * 1024;

	private char[] chars = new char[BUFFER_SIZE];

	private enum Token {
		PRIORITY, TIMESTAMP, HOSTNAME, MESSAGE
	}

	public SyslogMessage parse(Buffer buffer) {
		int priority = -1;
		int facility = -1;
		int severity = -1;
		String timestamp = null;
		String host = "localhost";
		String msg = null;

		Token t = Token.PRIORITY;
		int start = 0;
		int pos = 0;
		while (buffer.remaining() > 0) {
			if (pos + 1 > BUFFER_SIZE) {
				return null;
			}
			switch ((chars[pos++] = (char) buffer.read())) {
				case '<': {
					// start priority
					start = pos;
					break;
				}
				case '>': {
					priority = Integer.parseInt(new String(chars, start, (pos - 1) - start));
					facility = priority / 8;
					severity = priority % 8;

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

							timestamp = new String(chars, start, (pos - 1) - start);

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

							host = new String(chars, start, (pos - 1) - start).trim();
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
		msg = new String(chars, start, pos - start);

		return new SyslogMessage(priority, facility, severity, timestamp, host, msg);
	}

}
