package reactor.tcp.encoding;

import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public class PassThroughCodec implements Codec<Buffer, Buffer, Buffer> {
	@Override
	public Function<Buffer, Buffer> decoder(final Consumer<Buffer> next) {
		return new Function<Buffer, Buffer>() {
			@Override
			public Buffer apply(Buffer buffer) {
				next.accept(buffer);
				return null;
			}
		};
	}

	@Override
	public Function<Buffer, Buffer> encoder() {
		return new Function<Buffer, Buffer>() {
			@Override
			public Buffer apply(Buffer buffer) {
				return buffer;
			}
		};
	}
}
