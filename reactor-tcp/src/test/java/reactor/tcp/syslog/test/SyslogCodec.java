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

package reactor.tcp.syslog.test;

import reactor.function.Function;
import reactor.io.Buffer;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * @author Jon Brisbin
 */
public class SyslogCodec  {

	private final Charset utf8 = Charset.forName("UTF-8");

	//@Override
	public Function<Buffer, SyslogMessage> decoder() {
		return new SyslogMessageDecoder();
	}

	//@Override
	public Function<Void, Buffer> encoder() {
		return new Function<Void, Buffer>() {
			@Override
			public Buffer apply(Void aVoid) {
				return null;
			}
		};
	}

	private class SyslogMessageDecoder implements Function<Buffer, SyslogMessage> {
		private final CharsetDecoder decoder = utf8.newDecoder();

		@Override
		public SyslogMessage apply(Buffer buffer) {
			try {
				String s = decoder.decode(buffer.byteBuffer()).toString();
				return SyslogMessageParser.parse(s);
			} catch (CharacterCodingException e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
		}
	}

}
