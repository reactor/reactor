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

import reactor.function.Consumer;
import reactor.function.Function;

/**
 * Implementations of a {@literal Codec} are responsible for decoding a {@code SRC} into an
 * instance of {@code IN} and passing that to the given {@link reactor.function.Consumer}. A
 * codec also provides an encoder to take an instance of {@code OUT} and encode to an
 * instance of {@code SRC}.
 *
 * @param <SRC> The type that the codec decodes from and encodes to
 * @param <IN> The type produced by decoding
 * @param <OUT> The type consumed by encoding
 *
 * @author Jon Brisbin
 */
public interface Codec<SRC, IN, OUT> {

	/**
	 * Provide the caller with a decoder to turn a source object into an instance of the input
	 * type.
	 *
	 * @param next The {@link Consumer} to call after the object has been decoded.
	 * @return The decoded object.
	 */
	Function<SRC, IN> decoder(Consumer<IN> next);

	/**
	 * Provide the caller with an encoder to turn an output object into an instance of the source
	 * type.
	 *
	 * @return The encoded source object.
	 */
	Function<OUT, SRC> encoder();

}
