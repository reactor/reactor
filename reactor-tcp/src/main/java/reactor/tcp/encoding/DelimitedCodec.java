package reactor.tcp.encoding;

import reactor.fn.Function;
import reactor.io.Buffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
		private final Function<Buffer, IN> decoder   = delegate.decoder();
		private final Buffer               remainder = new Buffer();
		private final List<IN>             objs      = new ArrayList<IN>();
		private final List<Integer>        positions = new ArrayList<Integer>();

		@Override
		public Collection<IN> apply(Buffer bytes) {
			if (bytes.remaining() == 0) {
				return null;
			}
			objs.clear();
			positions.clear();

			while (bytes.remaining() > 0) {
				if (bytes.read() == delimiter) {
					positions.add(bytes.position() - 1);
				}
			}
			int end = bytes.position();

			if (!positions.isEmpty()) {
				int start = 0;
				for (Integer pos : positions) {
					bytes.byteBuffer().limit(pos);
					bytes.byteBuffer().position(start);
					IN in = decoder.apply(bytes);
					if (null != in) {
						objs.add(in);
					}
					start = pos + 1;
				}
			}

			if (bytes.position() + 1 < end) {
				remainder.append(bytes);
			}

			return (objs.isEmpty() ? null : Collections.unmodifiableList(objs));
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
