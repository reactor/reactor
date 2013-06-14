/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.tcp.encoding;

import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.Buffer;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class LengthFieldCodec<IN, OUT> implements Codec<Buffer, IN, OUT> {

	private final int                    lengthFieldLength;
	private final Codec<Buffer, IN, OUT> delegate;

	/**
	 * Create a length-field codec that reads the first integer as the length of the remaining message.
	 *
	 * @param delegate The delegate {@link Codec}.
	 */
	public LengthFieldCodec(Codec<Buffer, IN, OUT> delegate) {
		this(4, delegate);
	}

	/**
	 * Create a length-field codec that reads either the first integer or the first long as the the length of the remaining
	 * message.
	 *
	 * @param lengthFieldLength The length of the length field. Valid values are 4 (int) or 8 (long).
	 * @param delegate          The delegate {@link Codec}.
	 */
	public LengthFieldCodec(int lengthFieldLength, Codec<Buffer, IN, OUT> delegate) {
		Assert.state(lengthFieldLength == 4 || lengthFieldLength == 8, "lengthFieldLength should either be 4 (int) or 8 (long).");
		this.lengthFieldLength = lengthFieldLength;
		this.delegate = delegate;
	}

	@Override
	public Function<Buffer, IN> decoder(Consumer<IN> next) {
		return new LengthFieldDecoder(next);
	}

	@Override
	public Function<OUT, Buffer> encoder() {
		return new LengthFieldEncoder();
	}

	private class LengthFieldDecoder implements Function<Buffer, IN> {
		private final Function<Buffer, IN> decoder;
		private       Buffer               remainder;

		private LengthFieldDecoder(Consumer<IN> next) {
			this.decoder = delegate.decoder(next);
		}

		@Override
		public IN apply(Buffer buffer) {
			if (buffer.remaining() == 0) {
				return null;
			}

			Buffer b;
			if (null != remainder) {
				b = remainder;
			} else {
				b = buffer;
			}

			while (b.remaining() > lengthFieldLength) {
				int len;
				if (lengthFieldLength == 4) {
					len = b.readInt();
				} else {
					len = (int) b.readLong();
				}
				if (len > b.remaining()) {
					b.position(b.position() - lengthFieldLength);
					remainder = buffer;
					return null;
				}

				int pos = b.position();
				int limit = b.limit();
				Buffer.View v = b.createView(pos, pos + len);
				IN in = decoder.apply(v.get());
				b.byteBuffer().limit(limit);
				b.position(pos + len);
				if (in != null) {
					return in;
				}
			}

			if (b == remainder) {
				remainder = null;
				return apply(buffer);
			}

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
			return encoded;
		}
	}

}
