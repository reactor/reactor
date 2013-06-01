package reactor.tcp.encoding.syslog;

import reactor.fn.Function;
import reactor.io.Buffer;
import reactor.tcp.encoding.Codec;

/**
 * @author Jon Brisbin
 */
public class SyslogCodec implements Codec<Buffer, SyslogMessage, Void> {

	private static final Function<Void, Buffer> ENDCODER = new Function<Void, Buffer>() {
		@Override
		public Buffer apply(Void v) {
			return null;
		}
	};

	@Override
	public Function<Buffer, SyslogMessage> decoder() {
		return new SyslogMessageDecoder();
	}

	@Override
	public Function<Void, Buffer> encoder() {
		return ENDCODER;
	}

	private class SyslogMessageDecoder implements Function<Buffer, SyslogMessage> {
		private final SyslogParser parser = new SyslogParser();

		@Override
		public SyslogMessage apply(Buffer buffer) {
			return parser.parse(buffer);
		}
	}

}
