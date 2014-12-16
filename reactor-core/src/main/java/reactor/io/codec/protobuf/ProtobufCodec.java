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

package reactor.io.codec.protobuf;

import com.google.protobuf.Message;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.codec.SerializationCodec;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Jon Brisbin
 */
public class ProtobufCodec<IN, OUT> extends SerializationCodec<Map<Class<?>, Message>, IN, OUT> {

	public ProtobufCodec() {
		this(true);
	}

	public ProtobufCodec(boolean lengthFieldFraming) {
		super(new ConcurrentHashMap<Class<?>, Message>(), lengthFieldFraming);
	}

	@Override
	protected Function<byte[], IN> deserializer(final Map<Class<?>, Message> messages,
	                                            final Class<IN> type,
	                                            final Consumer<IN> next) {
		Assert.isAssignable(Message.class,
		                    type,
		                    "Can only deserialize Protobuf messages. " +
				                    type.getName() +
				                    " is not an instance of " +
				                    Message.class.getName());
		return new Function<byte[], IN>() {
			@SuppressWarnings("unchecked")
			@Override
			public IN apply(byte[] bytes) {
				try {
					Message msg = messages.get(type);
					if(null == msg) {
						msg = (Message)type.getMethod("getDefaultInstance").invoke(null);
						messages.put(type, msg);
					}
					IN obj = (IN)msg.newBuilderForType().mergeFrom(bytes).build();
					if(null != next) {
						next.accept(obj);
						return null;
					} else {
						return obj;
					}
				} catch(Exception e) {
					throw new IllegalStateException(e.getMessage(), e);
				}
			}
		};
	}

	@Override
	protected Function<OUT, byte[]> serializer(final Map<Class<?>, Message> messages) {
		return new Function<OUT, byte[]>() {
			@Override
			public byte[] apply(Object o) {
				Assert.isInstanceOf(Message.class,
				                    o,
				                    "Can only serialize Protobuf messages. " +
						                    o.getClass().getName() +
						                    " is not an instance of " +
						                    Message.class.getName());
				return ((Message)o).toByteArray();
			}
		};
	}

}
