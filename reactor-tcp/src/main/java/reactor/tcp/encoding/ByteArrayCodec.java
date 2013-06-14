package reactor.tcp.encoding;

import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.Buffer;

/**
 * A simple {@link Codec} implementation that turns a {@link Buffer} into a {@code byte[]} and visa-versa.
 *
 * @author Jon Brisbin
 */
public class ByteArrayCodec implements Codec<Buffer, byte[], byte[]> {

	@Override
	public Function<Buffer, byte[]> decoder(final Consumer<byte[]> next) {
		return new Function<Buffer, byte[]>() {
			@Override
			public byte[] apply(Buffer buffer) {
				if (null != next) {
					next.accept(buffer.asBytes());
					return null;
				} else {
					return buffer.asBytes();
				}
			}
		};
	}

	@Override
	public Function<byte[], Buffer> encoder() {
		return new Function<byte[], Buffer>() {
			@Override
			public Buffer apply(byte[] bytes) {
				return Buffer.wrap(bytes);
			}
		};
	}

}
