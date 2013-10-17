package reactor.queue.encoding;

import reactor.function.Function;
import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public class ByteArrayCodec implements Codec<byte[]> {

	private final Function<Buffer, byte[]> decoder = new Function<Buffer, byte[]>() {
		@Override
		public byte[] apply(Buffer buffer) {
			return buffer.asBytes();
		}
	};
	private final Function<byte[], Buffer> encoder = new Function<byte[], Buffer>() {
		@Override
		public Buffer apply(byte[] bytes) {
			return Buffer.wrap(bytes);
		}
	};

	@Override
	public Function<Buffer, byte[]> decoder() {
		return decoder;
	}

	@Override
	public Function<byte[], Buffer> encoder() {
		return encoder;
	}

}
