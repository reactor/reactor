package reactor.tcp.encoding;

import reactor.fn.Function;
import reactor.io.Buffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Jon Brisbin
 */
public class DelimitedCodec<IN, OUT> implements Codec<Buffer, Collection<IN>, Collection<OUT>> {

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
	public Function<Buffer, Collection<IN>> decoder() {
		return new DelimitedDecoder();
	}

	@Override
	public Function<Collection<OUT>, Buffer> encoder() {
		return new DelimitedEncoder();
	}

	public class DelimitedDecoder implements Function<Buffer, Collection<IN>> {
		private final Function<Buffer, IN> decoder = delegate.decoder();
		private Buffer remainder;

		@Override
		public Collection<IN> apply(Buffer bytes) {
			if (bytes.remaining() == 0) {
				return null;
			}

			if (null != remainder) {
				bytes.prepend(remainder);
			}

			List<IN> objs = new ArrayList<IN>();
			for (Buffer b : bytes.split(delimiter, false)) {
				if (b.last() == delimiter) {
					objs.add(decoder.apply(b));
				} else {
					// remainder
					remainder = new Buffer(b.byteBuffer().duplicate());
				}
			}

			return (objs.isEmpty() ? null : objs);
		}
	}

	public class DelimitedEncoder implements Function<Collection<OUT>, Buffer> {
		Function<OUT, Buffer> encoder = delegate.encoder();

		@Override
		public Buffer apply(Collection<OUT> out) {
			if (out.isEmpty()) {
				return null;
			}

			Buffer buffer = new Buffer();
			for (OUT o : out) {
				Buffer encoded = encoder.apply(o);
				if (null != encoded && encoded.remaining() > 0) {
					buffer.append(encoded).append(delimiter);
				}
			}

			return buffer.flip();
		}
	}

}
