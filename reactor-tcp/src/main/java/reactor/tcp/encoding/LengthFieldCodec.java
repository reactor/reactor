package reactor.tcp.encoding;

import reactor.fn.Function;
import reactor.fn.Observable;
import reactor.io.Buffer;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class LengthFieldCodec<IN, OUT> implements Codec<Buffer, IN, OUT> {

	private final int                    lengthFieldLength;
	private final Codec<Buffer, IN, OUT> delegate;

	public LengthFieldCodec(Codec<Buffer, IN, OUT> delegate) {
		this(4, delegate);
	}

	public LengthFieldCodec(int lengthFieldLength, Codec<Buffer, IN, OUT> delegate) {
		Assert.state(lengthFieldLength == 4 || lengthFieldLength == 8, "lengthFieldLength should either be 4 (int) or 8 (long).");
		this.lengthFieldLength = lengthFieldLength;
		this.delegate = delegate;
	}

	@Override
	public Function<Buffer, IN> decoder(Object notifyKey, Observable observable) {
		return new LengthFieldDecoder(notifyKey, observable);
	}

	@Override
	public Function<OUT, Buffer> encoder() {
		return new LengthFieldEncoder();
	}

	private class LengthFieldDecoder implements Function<Buffer, IN> {
		private final Function<Buffer, IN> decoder;
		private final Buffer remainder = new Buffer();

		private LengthFieldDecoder(Object notifyKey, Observable observable) {
			this.decoder = delegate.decoder(notifyKey, observable);
		}

		@Override
		public IN apply(Buffer buffer) {
			return null;
		}
	}

	private class LengthFieldEncoder implements Function<OUT, Buffer> {
		private final Function<OUT, Buffer> encoder = delegate.encoder();

		@Override
		public Buffer apply(OUT out) {
			if (null == out) {
				return null;
			}

			Buffer encoded = encoder.apply(out);
			if (null != encoded && encoded.remaining() > 0) {
				if (lengthFieldLength == 4) {
					encoded.prepend(encoded.remaining());
				} else if (lengthFieldLength == 8) {
					encoded.prepend((long) encoded.remaining());
				}
			}

			return null;
		}
	}

}
