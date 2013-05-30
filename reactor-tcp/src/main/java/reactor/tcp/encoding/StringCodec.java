package reactor.tcp.encoding;

import reactor.fn.Function;
import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public class StringCodec implements Codec<Buffer, String, String> {

	public static final Function<Buffer, String> DECODER = new StringDecoder();
	public static final Function<String, Buffer> ENCODER = new StringEncoder();

	@Override
	public Function<Buffer, String> decoder() {
		return DECODER;
	}

	@Override
	public Function<String, Buffer> encoder() {
		return ENCODER;
	}

	private static class StringDecoder implements Function<Buffer, String> {
		@Override
		public String apply(Buffer bytes) {
			return bytes.asString();
		}
	}

	private static class StringEncoder implements Function<String, Buffer> {
		@Override
		public Buffer apply(String s) {
			return Buffer.wrap(s);
		}
	}

}
