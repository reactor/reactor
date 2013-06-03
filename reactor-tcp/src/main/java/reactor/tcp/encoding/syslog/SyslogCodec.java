package reactor.tcp.encoding.syslog;

import reactor.fn.Function;
import reactor.io.Buffer;
import reactor.tcp.encoding.Codec;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

/**
 * @author Jon Brisbin
 */
public class SyslogCodec implements Codec<Buffer, Collection<SyslogMessage>, Void> {

	private static final Function<Void, Buffer> ENDCODER = new Function<Void, Buffer>() {
		@Override
		public Buffer apply(Void v) {
			return null;
		}
	};

	@Override
	public Function<Buffer, Collection<SyslogMessage>> decoder() {
		return new SyslogMessageDecoder();
	}

	@Override
	public Function<Void, Buffer> encoder() {
		return ENDCODER;
	}

	private class SyslogMessageDecoder implements Function<Buffer, Collection<SyslogMessage>> {
		private final CharsetDecoder charDecoder = Charset.forName("UTF-8").newDecoder();
		private final Calendar       calendar    = Calendar.getInstance();
		private final int            year        = calendar.get(Calendar.YEAR);
		private ByteBuffer remainder;

		@Override
		public Collection<SyslogMessage> apply(Buffer buffer) {
			return parse(buffer);
		}

		private Collection<SyslogMessage> parse(Buffer buffer) {
			boolean hasRemainder = (buffer.last() != '\n');
			int lastLf = -1;

			Collection<SyslogMessage> msgs = new ArrayList<SyslogMessage>();

			ByteBuffer bb = (null != remainder ? remainder : buffer.byteBuffer());
			while (null != bb) {
				int limit = bb.limit();
				int start = bb.position();
				int pos = start;
				while (bb.hasRemaining()) {
					byte b = bb.get();
					pos++;
					if (b == '\n') {
						lastLf = pos;
						if (pos >= limit) {
							break;
						}
						bb.position(start);
						bb.limit(pos);
						int len = pos - start;

						String s;
						try {
							s = charDecoder.decode(bb).toString();
						} catch (CharacterCodingException e) {
							throw new IllegalStateException(e);
						}
						int priStart = -1,
								priEnd = -1,
								tstampStart = -1,
								tstampEnd = -1,
								hostStart = -1,
								hostEnd = -1,
								msgStart = 0;
						boolean inMsg = false;
						for (int i = 0; i < len; i++) {
							char c = s.charAt(i);

							if (i == 0 && c == '<') {
								// start priority
								priStart = 1;
								while (s.charAt(++i) != '>') {
								}
								priEnd = i;
								continue;
							}

							if (c >= 'A' && c <= 'Z') {
								// start tstamp
								tstampStart = i;
								tstampEnd = i + 15;
								// skip past timestamp + space
								hostStart = (i += 16);
								// get hostname
								while (s.charAt(++i) != ' ') {
								}
								hostEnd = i;
								continue;
							}

							msgStart = i;
							break;
						}

						msgs.add(new SyslogMessage(s,
																			 priStart, priEnd,
																			 tstampStart, tstampEnd,
																			 hostStart, hostEnd,
																			 msgStart));

						bb.limit(limit);
						start = pos;
					}
				}

				if (bb == remainder) {
					bb = buffer.byteBuffer();
					remainder = null;
				} else {
					bb = null;
				}
			}

			if (hasRemainder && lastLf > 0 && lastLf < buffer.byteBuffer().limit()) {
				buffer.byteBuffer().position(lastLf);
				remainder = buffer.byteBuffer().duplicate();
			} else {
				remainder = null;
			}

			return msgs;
		}
	}

}
