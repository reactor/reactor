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

package reactor.io.codec.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.BufferCodec;
import reactor.io.codec.Codec;

import java.io.IOException;

/**
 * A codec for decoding JSON into Java objects and encoding Java objects into JSON.
 *
 * @param <IN>  The type to decode JSON into
 * @param <OUT> The type to encode into JSON
 * @author Jon Brisbin
 */
public class JsonCodec<IN, OUT> extends BufferCodec<IN, OUT> {

	private final Class<IN>    inputType;
	private final ObjectMapper mapper;

	/**
	 * Creates a new {@code JsonCodec} that will create instances of {@code inputType}  when
	 * decoding.
	 *
	 * @param inputType The type to create when decoding.
	 */
	public JsonCodec(Class<IN> inputType) {
		this(inputType, null);
	}

	/**
	 * Creates a new {@code JsonCodec} that will create instances of {@code inputType}  when
	 * decoding. The {@code customModule} will be registered with the underlying {@link
	 * ObjectMapper}.
	 *
	 * @param inputType    The type to create when decoding.
	 * @param customModule The module to register with the underlying ObjectMapper
	 */
	@SuppressWarnings("unchecked")
	public JsonCodec(Class<IN> inputType, Module customModule) {
		this(inputType, customModule, Codec.DEFAULT_DELIMITER);
	}

	/**
	 * Creates a new {@code JsonCodec} that will create instances of {@code inputType}  when
	 * decoding. The {@code customModule} will be registered with the underlying {@link
	 * ObjectMapper}.
	 *
	 * @param inputType    The type to create when decoding.
	 * @param customModule The module to register with the underlying ObjectMapper
	 * @param delimiter    A nullable delimiting byte for batch decoding
	 */
	@SuppressWarnings("unchecked")
	public JsonCodec(Class<IN> inputType, Module customModule, Byte delimiter) {
		super(delimiter);
		Assert.notNull(inputType, "inputType must not be null");
		this.inputType = inputType;

		this.mapper = new ObjectMapper();
		if (null != customModule) {
			this.mapper.registerModule(customModule);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected IN doBufferDecode(Buffer buffer) {
		try {
			if (JsonNode.class.isAssignableFrom(inputType)) {
				return (IN) mapper.readTree(buffer.inputStream());
			} else {
				return mapper.readValue(buffer.inputStream(), inputType);
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Function<Buffer, IN> decoder(Consumer<IN> next) {
		return new JsonDecoder(next);
	}

	private class JsonDecoder implements Function<Buffer, IN> {
		private final Consumer<IN> next;

		private JsonDecoder(Consumer<IN> next) {
			this.next = next;
		}

		@SuppressWarnings("unchecked")
		@Override
		public IN apply(Buffer buffer) {
			return doDelimitedBufferDecode(next, buffer);
		}
	}

	@Override
	public Buffer apply(OUT out) {
		try {
			return addDelimiterIfAny(new Buffer().append(mapper.writeValueAsBytes(out)));
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}
	}

}
