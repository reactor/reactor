package reactor.tcp.encoding.syslog;

import reactor.fn.Function;
import reactor.io.Buffer;
import reactor.tcp.encoding.Codec;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
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
		private final Charset        utf8    = Charset.forName("UTF-8");
		private final CharsetDecoder decoder = utf8.newDecoder();
		private final CharBuffer     cb      = CharBuffer.allocate(Buffer.MAX_BUFFER_SIZE);
		private Buffer remainder;

		@Override
		public Collection<SyslogMessage> apply(Buffer buffer) {
			return parse(buffer);
		}

		private Collection<SyslogMessage> parse(Buffer buffer) {
			cb.rewind();
			cb.limit(cb.capacity());

			boolean hasRemainder = (buffer.last() != '\n');
			ByteBuffer bb = buffer.byteBuffer();

			Collection<SyslogMessage> msgs = new ArrayList<SyslogMessage>();

			int limit = bb.limit();
			int start = bb.position();
			int pos = start;

			while (bb.hasRemaining()) {
				byte b = bb.get();
				pos++;
				if (b == '\n') {
					if (pos >= limit) {
						break;
					}
					bb.position(start);
					bb.limit(pos);

					String s;
					try {
						s = decoder.decode(bb).toString();
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
					int len = s.length();
					for (int i = 0; i < len; i++) {
						char c = s.charAt(i);
						if (i == 0 && c == '<') {
							// start priority
							priStart = 1;
							continue;
						} else if (priStart > 0 && c == '>') {
							// end priority
							priEnd = i;
							continue;
						}

						if (c >= 'A' && c <= 'Z') {
							// start tstamp
							tstampStart = i;
							tstampEnd = i + 15;
							// skip past timstamp + space
							hostStart = (i += 16);
							continue;
						}

						if (c == ' ') {
							// end of hostname
							hostEnd = i;
							msgStart = ++i;
							break;
						}
					}

					msgs.add(new SyslogMessage(s, priStart, priEnd, tstampStart, tstampEnd, hostStart, hostEnd, msgStart));

					bb.limit(limit);
					start = pos;
				}
			}

			if (hasRemainder) {
				remainder = buffer;
			} else {
				remainder = null;
			}

			return msgs;
		}
	}

}
