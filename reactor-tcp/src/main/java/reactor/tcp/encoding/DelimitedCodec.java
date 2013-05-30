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
		private final Function<Buffer, IN> decoder = delegate.decoder();
		private       int                  start   = 0;
		private Buffer remainder;

		@Override
		public IN apply(Buffer bytes) {
			if (null != remainder) {
				remainder.append(bytes).flip();
				bytes = remainder;
			}

			int limit = bytes.byteBuffer().limit();
			while (bytes.remaining() > 0) {
				int pos = bytes.position();
				byte b;
				if ((b = bytes.read()) == delimiter) {
					bytes.byteBuffer().position(start);
					bytes.byteBuffer().limit((pos - 1) - start);

					IN in = decoder.apply(bytes);
					start = bytes.position();
					remainder = null;

					bytes.byteBuffer().limit(limit);
					bytes.byteBuffer().position(pos);

					return in;
				} else if (b == '\0' || pos == limit) {
					break;
				}
			}

			int pos = bytes.position();
			if ((pos - start) > 0) {
				bytes.byteBuffer().position(start);
				remainder = bytes;
			}

			start = 0;
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
