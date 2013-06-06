package reactor.tcp.encoding;

import reactor.fn.Function;
import reactor.fn.Observable;
import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public class DelimitedCodec<IN, OUT> implements Codec<Buffer, IN, OUT> {

	private final Codec<Buffer, IN, OUT> delegate;
	private final char                   delimiter;

	public DelimitedCodec(Codec<Buffer, IN, OUT> delegate) {
		this('\n', delegate);
	}

	public DelimitedCodec(char delimiter, Codec<Buffer, IN, OUT> delegate) {
		this.delimiter = delimiter;
		this.delegate = delegate;
	}

	@Override
	public Function<Buffer, IN> decoder(Object notifyKey, Observable observable) {
		return new DelimitedDecoder(notifyKey, observable);
	}

	@Override
	public Function<OUT, Buffer> encoder() {
		return new DelimitedEncoder();
	}

	public class DelimitedDecoder implements Function<Buffer, IN> {
		private final Function<Buffer, IN> decoder;
		private       Buffer               remainder;

		public DelimitedDecoder(Object notifyKey, Observable observable) {
			this.decoder = delegate.decoder(notifyKey, observable);
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
					remainder = new Buffer(b.byteBuffer().duplicate());
				}
			}

			return null;
		}
	}

	public class DelimitedEncoder implements Function<OUT, Buffer> {
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
