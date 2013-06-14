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

/**
 * @author Jon Brisbin
 */
public abstract class StandardCodecs {

	private StandardCodecs() {
	}

	public static final ByteArrayCodec                 BYTE_ARRAY_CODEC = new ByteArrayCodec();
	public static final StringCodec                    STRING_CODEC     = new StringCodec();
	public static final DelimitedCodec<String, String> LINE_FEED_CODEC  = new DelimitedCodec<String, String>(STRING_CODEC);

	public static <SRC,IN,OUT> Codec<SRC,IN,OUT> passthroughCodec(){
		return new Codec<SRC, IN, OUT>() {
			@Override
			public Function<SRC, IN> decoder(Consumer<IN> next) {
				return new Function<SRC, IN>() {
					@Override
					public IN apply(SRC src) {
						return null;
					}
				};
			}

			@Override
			public Function<OUT, SRC> encoder() {
				return new Function<OUT, SRC>() {
					@Override
					public SRC apply(OUT out) {
						return null;
					}
				};
			}
		};
	}
}
