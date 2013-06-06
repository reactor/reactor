package reactor.tcp.encoding.syslog;

import reactor.fn.Function;
import reactor.io.Buffer;
import reactor.tcp.encoding.Codec;

import java.util.*;

/**
 * @author Jon Brisbin
 */
public class SyslogCodec implements Codec<Buffer, Collection<SyslogMessage>, Void> {

	private static final int MAXIMUM_SEVERITY = 7;
	private static final int MAXIMUM_FACILITY = 23;
	private static final int MINIMUM_PRI      = 0;
	private static final int MAXIMUM_PRI      = (MAXIMUM_FACILITY * 8) + MAXIMUM_SEVERITY;
	private static final int DEFAULT_PRI      = 13;

	private static final Function<Void, Buffer> ENDCODER = new Function<Void, Buffer>() {
		@Override
		public Buffer apply(Void v) {
			return null;
		}
	};

	private Calendar cal  = Calendar.getInstance();
	private int      year = cal.get(Calendar.YEAR);

	@Override
	public Function<Buffer, Collection<SyslogMessage>> decoder() {
		return new SyslogMessageDecoder();
	}

	@Override
	public Function<Void, Buffer> encoder() {
		return ENDCODER;
	}

	private class SyslogMessageDecoder implements Function<Buffer, Collection<SyslogMessage>> {
		private final List<Buffer.View> views = new ArrayList<Buffer.View>();
		private Buffer.View remainder;

		@Override
		public Collection<SyslogMessage> apply(Buffer buffer) {
			return parse(buffer);
		}

		private Collection<SyslogMessage> parse(Buffer buffer) {
			boolean hasRemainder = (buffer.last() != '\n');
			int lastLf = -1;

			Collection<SyslogMessage> msgs = new ArrayList<SyslogMessage>();

			String line = null;
			if (null != remainder) {
				line = remainder.get().asString();
			}

			int start = 0;
			for (Buffer.View view : buffer.split(views, '\n', false)) {
				Buffer b = view.get();
				String s = b.asString();
				if (null != line) {
					line += s;
				} else {
					line = s;
				}

				int priority = DEFAULT_PRI;
				int facility = priority / 8;
				int severity = priority % 8;

				int priStart = line.indexOf('<', start);
				int priEnd = line.indexOf('>', start + 1);
				if (priStart == 0) {
					int pri = Buffer.parseInt(b, 1, priEnd);
					if (pri >= MINIMUM_PRI && pri <= MAXIMUM_PRI) {
						priority = pri;
						facility = priority / 8;
						severity = priority % 8;
					}
					start = 4;
				}

//				Date tstamp = parseRfc3414Date(b, start, start + 15);
				String host = null;
//				if (null != tstamp) {
				start += 16;
				int end = line.indexOf(' ', start);
				host = line.substring(start, end);
				if (null != host) {
					start += host.length() + 1;
				}
//				}

				String msg = line.substring(start);

				msgs.add(new SyslogMessage(priority, facility, severity, null, host, msg));
			}

			if (hasRemainder && lastLf > 0 && lastLf < buffer.limit()) {
				remainder = buffer.createView(lastLf, buffer.limit());
			} else {
				remainder = null;
			}

			return msgs;
		}
	}

	private Date parseRfc3414Date(Buffer b, int start, int end) {
		int origPos = b.position();
		int origLimit = b.limit();

		b.byteBuffer().position(start);
		b.byteBuffer().limit(end);

		int month = -1;
		int day = -1;
		int hr = -1;
		int min = -1;
		int sec = -1;

		switch (b.read()) {
			case 'A': // Apr, Aug
				switch (b.read()) {
					case 'p':
						month = Calendar.APRIL;
						b.read();
						break;
					default:
						month = Calendar.AUGUST;
						b.read();
				}
				break;
			case 'D': // Dec
				month = Calendar.DECEMBER;
				b.read();
				b.read();
				break;
			case 'F': // Feb
				month = Calendar.FEBRUARY;
				b.read();
				b.read();
				break;
			case 'J': // Jan, Jun, Jul
				switch (b.read()) {
					case 'a':
						month = Calendar.JANUARY;
						b.read();
						break;
					default:
						switch (b.read()) {
							case 'n':
								month = Calendar.JUNE;
								break;
							default:
								month = Calendar.JULY;
						}
				}
				break;
			case 'M': // Mar, May
				b.read();
				switch (b.read()) {
					case 'r':
						month = Calendar.MARCH;
						break;
					default:
						month = Calendar.MAY;
				}
				break;
			case 'N': // Nov
				month = Calendar.NOVEMBER;
				b.read();
				b.read();
				break;
			case 'O': // Oct
				month = Calendar.OCTOBER;
				b.read();
				b.read();
				break;
			case 'S': // Sep
				month = Calendar.SEPTEMBER;
				b.read();
				b.read();
				break;
			default:
				return null;
		}

		while (b.read() == ' ') {
		}

		int dayStart = b.position() - 1;
		while (b.read() != ' ') {
		}
		int dayEnd = b.position() - 1;
		day = Buffer.parseInt(b, dayStart, dayEnd);

		while (b.read() == ' ') {
		}

		int timeStart = b.position() - 1;
		hr = Buffer.parseInt(b, timeStart, timeStart + 2);
		min = Buffer.parseInt(b, timeStart + 3, timeStart + 5);
		sec = Buffer.parseInt(b, timeStart + 6, timeStart + 8);

		try {
			if (month < 0 || day < 0 || hr < 0 || min < 0 || sec < 0) {
				return null;
			} else {
				cal.set(year, month, day, hr, min, sec);
				return cal.getTime();
			}
		} finally {
			b.byteBuffer().limit(origLimit);
			b.byteBuffer().position(origPos);
		}
	}

}
