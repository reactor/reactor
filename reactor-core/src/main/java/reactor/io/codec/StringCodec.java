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

/**
 * @author Jon Brisbin
 */
public class StringCodec extends Codec<Buffer, String, String> {

	private final Charset        utf8    = Charset.forName("UTF-8");
	private final CharsetDecoder decoder = utf8.newDecoder();

	public StringCodec() {
		this(null);
	}

	public StringCodec(Byte delimiter) {
		super(delimiter);
	}

	@Override
	public Function<Buffer, String> decoder(Consumer<String> next) {
		return new StringDecoder(next);
	}

	@Override
	protected String doBufferDecode(Buffer buffer) {
		try {
			return decoder.decode(buffer.byteBuffer()).toString();
		} catch (CharacterCodingException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Function<String, Buffer> encoder() {
		return new StringEncoder();
	}

	private class StringDecoder implements Function<Buffer, String> {
		private final Consumer<String> next;

		private StringDecoder(Consumer<String> next) {
			this.next = next;
		}

		@Override
		public String apply(Buffer bytes) {
			return doDelimitedBufferDecode(next, bytes);
		}
	}

	private class StringEncoder implements Function<String, Buffer> {
		private final CharsetEncoder encoder = utf8.newEncoder();

		@Override
		public Buffer apply(String s) {
			try {
				ByteBuffer bb = encoder.encode(CharBuffer.wrap(s));
				if(delimiter != null) {
					return addDelimiterIfAny(new Buffer().append(bb));
				}else{
					return new Buffer(bb);
				}
			} catch (CharacterCodingException e) {
				throw new IllegalStateException(e);
			}
		}
	}

}
