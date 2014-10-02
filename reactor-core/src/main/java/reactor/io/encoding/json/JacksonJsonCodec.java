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

package reactor.io.encoding.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.encoding.SerializationCodec;

import java.io.IOException;

/**
 * @author Jon Brisbin
 */
public class JacksonJsonCodec<IN, OUT> extends SerializationCodec<ObjectMapper, IN, OUT> {

	public JacksonJsonCodec() {
		this(new ObjectMapper());
	}

	public JacksonJsonCodec(ObjectMapper engine) {
		super(engine, true);
	}

	@Override
	protected Function<byte[], IN> deserializer(final ObjectMapper engine,
	                                            final Class<IN> type,
	                                            final Consumer<IN> next) {
		return new Function<byte[], IN>() {
			@Override
			public IN apply(byte[] bytes) {
				try {
					IN o = engine.readValue(bytes, type);
					if(null != next) {
						next.accept(o);
						return null;
					} else {
						return o;
					}
				} catch(IOException e) {
					throw new IllegalStateException(e.getMessage(), e);
				}
			}
		};
	}

	@Override
	protected Function<OUT, byte[]> serializer(final ObjectMapper engine) {
		return new Function<OUT, byte[]>() {
			@Override
			public byte[] apply(OUT o) {
				try {
					return engine.writeValueAsBytes(o);
				} catch(JsonProcessingException e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}
			}
		};
	}

}
