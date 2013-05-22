/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.tcp.codec;

import java.nio.ByteBuffer;

import reactor.fn.Consumer;
import reactor.tcp.data.Buffers;

/**
 * A {@code Codec} is used to decode raw data into a type, and to encode bytes into buffers with framing.
 *
 * @author Gary Russell
 * @author Andy Wilkinson
 *
 * @param T the type that will be decoded
 */
public interface Codec<T> {

	/**
	 * Decode the data in buffers into an assembled message, either from
	 * partial data in one buffer or by reassembling multiple buffers.
	 *
	 * @param buffers  the buffers
	 * @param consumer the consumer to invoke with each assembly
	 */
	void decode(Buffers buffers, Consumer<T> consumer);

	/**
	 * Add framing to data for transmittal.
	 *
	 * @return {@link Buffers} containing the data with framing.
	 */
	Buffers encode(ByteBuffer buffer);
}
