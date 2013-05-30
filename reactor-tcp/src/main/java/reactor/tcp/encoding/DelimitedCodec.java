package reactor.tcp.encoding;

import reactor.fn.Function;
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
	public Function<Buffer, IN> decoder() {
		return new DelimitedDecoder();
	}

	@Override
	public Function<OUT, Buffer> encoder() {
		return new DelimitedEncoder();
	}

	public class DelimitedDecoder implements Function<Buffer, IN> {
		final Function<Buffer, IN> decoder = delegate.decoder();
		int start = 0;

		@Override
		public IN apply(Buffer bytes) {
			while (bytes.remaining() > 0) {
				byte b;
				if ((b = bytes.read()) == delimiter) {
					IN in = decoder.apply(bytes.slice(start, (bytes.position() - 1) - start));
					start = bytes.position();
					return in;
				} else if (b == '\0') {
					return null;
				}
			}
			return null;
		}
	}

	public class DelimitedEncoder implements Function<OUT, Buffer> {
		Function<OUT, Buffer> encoder = delegate.encoder();

		@Override
		public Buffer apply(OUT out) {
			Buffer bytes = encoder.apply(out);
			bytes.append(delimiter);
			return bytes.flip();
		}
	}

}
