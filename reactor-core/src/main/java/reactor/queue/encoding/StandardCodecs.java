package reactor.queue.encoding;

import reactor.function.Function;
import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public abstract class StandardCodecs {

	private static final StringCodec    STRING_CODEC    = new StringCodec();
	private static final ByteArrayCodec BYTEARRAY_CODEC = new ByteArrayCodec();

	protected StandardCodecs() {
	}

	public static Codec<String> stringCodec() {
		return STRING_CODEC;
	}

	public static Codec<byte[]> bytesCodec() {
		return BYTEARRAY_CODEC;
	}

	public static Codec<Buffer> bufferCodec() {
		return new Codec<Buffer>() {
			private final Function<Buffer, Buffer> decoder = new Function<Buffer, Buffer>() {
				@Override
				public Buffer apply(Buffer buffer) {
					return buffer;
				}
			};
			private final Function<Buffer, Buffer> encoder = new Function<Buffer, Buffer>() {
				@Override
				public Buffer apply(Buffer buffer) {
					return buffer;
				}
			};

			@Override
			public Function<Buffer, Buffer> decoder() {
				return decoder;
			}

			@Override
			public Function<Buffer, Buffer> encoder() {
				return encoder;
			}
		};
	}

}
