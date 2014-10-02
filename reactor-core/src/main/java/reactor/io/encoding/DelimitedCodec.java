/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.encoding;

import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.Buffer;
import reactor.io.Buffer.View;
import reactor.io.encoding.Codec;

/**
 * An implementation of {@link reactor.io.encoding.Codec} that decodes by splitting a {@link Buffer} into segments
 * based on a delimiter and encodes by appending its delimiter to each piece of output.
 * During decoding the delegate is used to process each segment. During encoding the delegate
 * is used to create a buffer for each piece of output to which the delimiter is then appended.
 *
 * @param <IN> The type that will be produced by decoding
 * @param <OUT> The type that will be consumed by encoding
 *
 * @author Jon Brisbin
 */
public class DelimitedCodec<IN, OUT> implements Codec<Buffer, IN, OUT> {

	private final Codec<Buffer, IN, OUT> delegate;
	private final byte                   delimiter;
	private final boolean                stripDelimiter;

	/**
	 * Create a line-feed-delimited codec, using the given {@code Codec} as a delegate.
	 *
	 * @param delegate The delegate {@link Codec}.
	 */
	public DelimitedCodec(Codec<Buffer, IN, OUT> delegate) {
		this((byte) 10, true, delegate);

	}

	/**
	 * Create a line-feed-delimited codec, using the given {@code Codec} as a delegate.
	 *
	 * @param stripDelimiter Flag to indicate whether the delimiter should be stripped from the
	 *                       chunk or not during decoding.
	 * @param delegate       The delegate {@link Codec}.
	 */
	public DelimitedCodec(boolean stripDelimiter, Codec<Buffer, IN, OUT> delegate) {
		this((byte) 10, stripDelimiter, delegate);

	}

	/**
	 * Create a delimited codec using the given delimiter and using the given {@code Codec}
	 * as a delegate.
	 *
	 * @param delimiter      The delimiter to use.
	 * @param stripDelimiter Flag to indicate whether the delimiter should be stripped from the
	 *                       chunk or not during decoding.
	 * @param delegate       The delegate {@link Codec}.
	 */
	public DelimitedCodec(byte delimiter, boolean stripDelimiter, Codec<Buffer, IN, OUT> delegate) {
		this.delimiter = delimiter;
		this.stripDelimiter = stripDelimiter;
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

		DelimitedDecoder(Consumer<IN> next) {
			this.decoder = delegate.decoder(next);
		}

		@Override
		public IN apply(Buffer bytes) {
			if (bytes.remaining() == 0) {
				return null;
			}

			Iterable<View> views = bytes.split(delimiter, stripDelimiter);

			int limit = bytes.limit();
			int position = bytes.position();

			for (Buffer.View view : views) {
				Buffer b = view.get();
				decoder.apply(b);
			}

			bytes.limit(limit);
			bytes.position(position);

			return null;
		}
	}

	private class DelimitedEncoder implements Function<OUT, Buffer> {
		Function<OUT, Buffer> encoder = delegate.encoder();

		@Override
		@SuppressWarnings("resource")
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
