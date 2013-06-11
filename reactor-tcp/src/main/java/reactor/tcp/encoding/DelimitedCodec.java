package reactor.tcp.encoding;

import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.Buffer;

/**
 * An implementation of that splits a {@link Buffer} into segments based on a delimiter.
 *
 * @author Jon Brisbin
 */
public class DelimitedCodec<IN, OUT> implements Codec<Buffer, IN, OUT> {

	private final Codec<Buffer, IN, OUT> delegate;
	private final byte                   delimiter;

	/**
	 * Create a line-feed-delimited codec, using the given {@literal Codec} as a delegate.
	 *
	 * @param delegate The delegate {@link Codec}.
	 */
	public DelimitedCodec(Codec<Buffer, IN, OUT> delegate) {
		this((byte) 10, delegate);
	}

	/**
	 * Create a delimited codec using the given delimiter and using the given {@literal Codec} as a delegate.
	 *
	 * @param delimiter The delimiter to use.
	 * @param delegate  The delegate {@link Codec}.
	 */
	public DelimitedCodec(byte delimiter, Codec<Buffer, IN, OUT> delegate) {
		this.delimiter = delimiter;
		this.delegate = delegate;
	}

	@Override
	public Function<Buffer, IN> decoder(Consumer<IN> next) {
		return new DelimitedDecoder(next);
	}

	@Override
	public Function<OUT, Buffer> encoder() {
		return new DelimitedEncoder();
	}

	private class DelimitedDecoder implements Function<Buffer, IN> {
		private final Function<Buffer, IN> decoder;
		private       Buffer               remainder;

		public DelimitedDecoder(Consumer<IN> next) {
			this.decoder = delegate.decoder(next);
		}

		@Override
		public IN apply(Buffer bytes) {
			if (bytes.remaining() == 0) {
				return null;
			}

			if (null != remainder) {
				bytes.prepend(remainder);
			}

			for (Buffer.View view : bytes.split(delimiter, false)) {
				Buffer b = view.get();
				if (b.last() == delimiter) {
					decoder.apply(b);
				} else {
					// remainder
					remainder = b;
				}
			}

			return null;
		}
	}

	private class DelimitedEncoder implements Function<OUT, Buffer> {
		Function<OUT, Buffer> encoder = delegate.encoder();

		@Override
		public Buffer apply(OUT out) {
			Buffer buffer = new Buffer();
			Buffer encoded = encoder.apply(out);
			if (null != encoded && encoded.remaining() > 0) {
				buffer.append(encoded).append(delimiter);
			}
			return buffer.flip();
		}
	}

}
