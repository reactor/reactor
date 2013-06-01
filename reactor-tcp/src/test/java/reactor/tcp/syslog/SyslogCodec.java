package reactor.tcp.syslog;

import reactor.fn.Function;
import reactor.io.Buffer;
import reactor.tcp.encoding.Codec;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * @author Jon Brisbin
 */
public class SyslogCodec implements Codec<Buffer, SyslogMessage, Void> {

	private final Charset utf8 = Charset.forName("UTF-8");

	@Override
	public Function<Buffer, SyslogMessage> decoder() {
		return new SyslogMessageDecoder();
	}

	@Override
	public Function<Void, Buffer> encoder() {
		return new Function<Void, Buffer>() {
			@Override
			public Buffer apply(Void aVoid) {
				return null;
			}
		};
	}

	private class SyslogMessageDecoder implements Function<Buffer, SyslogMessage> {
		private final CharsetDecoder decoder = utf8.newDecoder();

		@Override
		public SyslogMessage apply(Buffer buffer) {
			try {
				String s = decoder.decode(buffer.byteBuffer()).toString();
				return SyslogMessageParser.parse(s);
			} catch (CharacterCodingException e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
		}
	}

}
