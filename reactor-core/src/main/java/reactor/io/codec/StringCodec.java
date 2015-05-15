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

package reactor.io.codec;

import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.List;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class StringCodec extends Codec<Buffer, String, String> {

	private final Charset        charset;

	public StringCodec() {
		this(null, Charset.forName("UTF-8"));
	}

	public StringCodec(Charset charset) {
		this(null, charset);
	}

	public StringCodec(Byte delimiter) {
		this(delimiter, Charset.forName("UTF-8"));
	}

	public StringCodec(Byte delimiter, Charset charset) {
		super(delimiter);
		this.charset = charset;
	}

	@Override
	public Function<String, Buffer> encoder() {
		return new StringEncoder();
	}

	@Override
	public Function<Buffer, String> decoder(Consumer<String> next) {
		return new StringDecoder(next);
	}

	@Override
	protected String doBufferDecode(Buffer buffer) {
		return decode(buffer, charset.newDecoder());
	}

	@Override
	public Buffer apply(String s) {
		return encode(s, charset.newEncoder());
	}

	protected String decode(Buffer buffer, CharsetDecoder charsetDecoder){
		try {
			return charsetDecoder.decode(buffer.byteBuffer()).toString();
		} catch (CharacterCodingException e) {
			throw new IllegalStateException(e);
		}
	}

	private class StringDecoder implements Function<Buffer, String> {

		private final CharsetDecoder decoder;
		private final Consumer<String> next;

		private StringDecoder(Consumer<String> next) {
			this.next = next;
			this.decoder = charset.newDecoder();
		}

		@Override
		public String apply(Buffer buffer) {
			//split using the delimiter
			if(delimiter != null) {
				List<Buffer.View> views = buffer.split(delimiter);
				int viewCount = views.size();

				if (viewCount == 0) return invokeCallbackOrReturn(next, doBufferDecode(buffer));

				for (Buffer.View view : views) {
					String in = invokeCallbackOrReturn(next, decode(view.get(), decoder));
					if(in != null) return in;
				}
				return null;
			}else{
				return invokeCallbackOrReturn(next, decode(buffer, decoder));
			}
		}
	}

	protected Buffer encode(String s, CharsetEncoder charsetEncoder){
		try {
			ByteBuffer bb = charsetEncoder.encode(CharBuffer.wrap(s));
			if (delimiter != null) {
				return addDelimiterIfAny(new Buffer().append(bb));
			} else {
				return new Buffer(bb);
			}
		} catch (CharacterCodingException e) {
			throw new IllegalStateException(e);
		}
	}

	public final class StringEncoder implements Function<String, Buffer> {
		private final CharsetEncoder encoder = charset.newEncoder();

		@Override
		public Buffer apply(String s) {
			return encode(s, encoder);
		}
	}

}
