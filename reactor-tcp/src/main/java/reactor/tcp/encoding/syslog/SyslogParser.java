package reactor.tcp.encoding.syslog;

import reactor.io.Buffer;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jon Brisbin
 */
public class SyslogParser {

	private static final int MAXIMUM_SEVERITY = 7;
	private static final int MAXIMUM_FACILITY = 23;
	private static final int MINIMUM_PRI      = 0;
	private static final int MAXIMUM_PRI      = (MAXIMUM_FACILITY * 8) + MAXIMUM_SEVERITY;
	private static final int DEFAULT_PRI      = 13;

	private final Charset        utf8    = Charset.forName("UTF-8");
	private final CharsetDecoder decoder = utf8.newDecoder();
	private final Pattern        pattern = Pattern.compile("^(<(\\d{1,3})>)?(\\w{3}[\\s]+[\\d]{1,2}[\\s]+\\d\\d:\\d\\d:\\d\\d)?[\\s]*(\\w*)[\\s]*(.+)\n");
	private Matcher matcher;

//	public SyslogMessage parse(Buffer buffer) {
//		if (null == buffer || buffer.remaining() == 0) {
//			return null;
//		}
//
//		int priority = DEFAULT_PRI;
//		int facility = -1;
//		int severity = -1;
//		String timestamp = null;
//		String host = "localhost";
//		String msg = null;
//
//		String syslogMsg;
//		try {
//			syslogMsg = decoder.decode(buffer.byteBuffer()).toString();
//		} catch (CharacterCodingException e) {
//			throw new IllegalArgumentException(e.getMessage(), e);
//		}
//
//		if (null == matcher) {
//			matcher = pattern.matcher(syslogMsg);
//		} else {
//			matcher.reset(syslogMsg);
//		}
//		if (matcher.matches()) {
//			if (null != matcher.group(2)) {
//				int i = Integer.valueOf(matcher.group(2));
//				if (i > MINIMUM_PRI && i <= MAXIMUM_PRI) {
//					priority = i;
//				}
//			}
//			facility = priority / 8;
//			severity = priority % 8;
//
//			timestamp = matcher.group(3);
//			host = matcher.group(4);
//			msg = matcher.group(5);
//		}
//
//		return new SyslogMessage(priority, facility, severity, timestamp, host, msg);
//	}

	public SyslogMessage parse(Buffer buffer) {
		if (null == buffer || buffer.remaining() == 0) {
			return null;
		}

		int priority = DEFAULT_PRI;
		int facility = -1;
		int severity = -1;
		String timestamp = null;
		String host = "localhost";
		String msg = null;

		String syslogMsg;
		try {
			syslogMsg = decoder.decode(buffer.byteBuffer()).toString();
		} catch (CharacterCodingException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}

		int pos;
		if (syslogMsg.indexOf('<') != 0) {
			throw new IllegalStateException("Invalid syslog message format: " + syslogMsg.substring(0, 10) + "...");
		}
		int endPri = syslogMsg.indexOf('>');
		if (endPri >= 2 && endPri <= 4) {
			int candidatePri = Integer.parseInt(syslogMsg.substring(1, endPri));
			if (candidatePri >= MINIMUM_PRI && candidatePri <= MAXIMUM_PRI) {
				priority = candidatePri;
				facility = priority / 8;
				severity = priority % 8;

				pos = endPri + 1;
				timestamp = syslogMsg.substring(pos, pos + 15);
				pos += 16;

				int endHostnameIndex = syslogMsg.indexOf(' ', pos);
				host = syslogMsg.substring(pos, endHostnameIndex);
			}
		}

		if (host == null) {
			// TODO Need a way to populate host with client's IP address
		}


		if (timestamp == null) {
			//timestamp = new Timestamp(Calendar.getInstance());
		}

		return null;
	}

}
