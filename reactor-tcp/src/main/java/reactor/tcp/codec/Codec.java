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

import reactor.tcp.Buffers;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
public interface Codec {

	/**
	 * Decode the data in buffers into an assembled message, either from
	 * partial data in one buffer or by reassembling multiple buffers.
	 * @param buffers The buffers.
	 * @param callback a callback to invoke for each assembly
	 */
	void decode(Buffers buffers, DecoderCallback callback);

	/**
	 * Add framing to data for transmittal.
	 * @return a collection of ByteBuffers containing the data with framing.
	 */
	Buffers encode(ByteBuffer buffer);


	interface DecoderCallback {

		void complete(DecoderResult result);
	}
}
