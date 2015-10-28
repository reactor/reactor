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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;
import reactor.io.buffer.StringBuffer;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class StringCodec extends BufferCodec<String, String> {

	private final Charset charset;

	public StringCodec() {
		this(null, Charset.forName("UTF-8"));
	}

	public StringCodec(Charset charset) {
		this(null, charset);
	}

	public StringCodec(Byte delimiter) {
		this(delimiter, Charset.forName("UTF-8"));
	}

	public StringCodec(Byte delimiter, final Charset charset) {
		super(delimiter, new Supplier<CharsetDecoder>(){
			@Override
			public CharsetDecoder get() {
				return charset.newDecoder();
			}
		});
		this.charset = charset;
	}

	@Override
	public Function<String, Buffer> encoder() {
		return new StringEncoder();
	}

	@Override
	@SuppressWarnings("unchecked")
	protected String decodeNext(Buffer buffer, Object context) {
		Buffer b = buffer;
		if(delimiter != null){
			int end = buffer.indexOf(delimiter);
			if(end != - 1) {
				b = buffer.duplicate().limit(end - 1);
				buffer.skip((Math.min(buffer.limit(), end + 1) - buffer.position()));
			}
		}
		return decode(b, (CharsetDecoder)context);
	}



	@Override
	public Buffer apply(String s) {
		return encode(s, charset.newEncoder());
	}

	protected String decode(Buffer buffer, CharsetDecoder charsetDecoder) {
		try {
			return charsetDecoder.decode(buffer.byteBuffer()).toString();
		} catch (CharacterCodingException e) {
			throw new IllegalStateException(e);
		}
	}

	protected Buffer encode(String s, CharsetEncoder charsetEncoder) {
		try {
			ByteBuffer bb = charsetEncoder.encode(CharBuffer.wrap(s));
			if (delimiter != null) {
				return addDelimiterIfAny(new StringBuffer().append(bb));
			} else {
				return new StringBuffer(bb);
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
