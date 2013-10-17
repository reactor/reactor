package reactor.queue.encoding;

import reactor.function.Function;
import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public class StringCodec implements Codec<String> {

	private final Function<Buffer, String> decoder = new Function<Buffer, String>() {
		@Override
		public String apply(Buffer buffer) {
			return buffer.asString();
		}
	};
	private final Function<String, Buffer> encoder = new Function<String, Buffer>() {
		@Override
		public Buffer apply(String s) {
			return Buffer.wrap(s);
		}
	};

	@Override
	public Function<Buffer, String> decoder() {
		return decoder;
	}

	@Override
	public Function<String, Buffer> encoder() {
		return encoder;
	}

}
