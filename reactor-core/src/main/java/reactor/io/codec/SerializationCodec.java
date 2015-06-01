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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract base class for {@code Codec Codecs} that perform serialization of objects. Optionally handles writing class
 * names so that an object that is serialized can be properly instantiated with full type information on the other end.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class SerializationCodec<E, IN, OUT> extends BufferCodec<IN, OUT> {

	private final Logger                 log   = LoggerFactory.getLogger(getClass());
	private final Map<String, Class<IN>> types = new ConcurrentHashMap<String, Class<IN>>();
	private final E                      engine;
	private final boolean                lengthFieldFraming;
	private final Codec<Buffer, IN, OUT> encoder;

	/**
	 * Create a {@code SerializationCodec} using the given engine and specifying whether or not to prepend a length field
	 * to frame the message.
	 *
	 * @param engine             the engine which will perform the serialization
	 * @param lengthFieldFraming {@code true} to prepend a length field, or {@code false} to skip
	 */
	protected SerializationCodec(E engine, boolean lengthFieldFraming) {
		this.engine = engine;
		this.lengthFieldFraming = lengthFieldFraming;
		if (lengthFieldFraming) {
			this.encoder = new LengthFieldCodec<IN, OUT>(new DelegateCodec());
		} else {
			this.encoder = new DelegateCodec();
		}
	}

	@Override
	public Function<Buffer, IN> decoder(Consumer<IN> next) {
		if (lengthFieldFraming) {
			return new LengthFieldCodec<IN, OUT>(new DelegateCodec()).decoder(next);
		} else {
			return new DelegateCodec().decoder(next);
		}
	}

	@Override
	public Buffer apply(OUT out) {
		return encoder.apply(out);
	}

	protected E getEngine() {
		return engine;
	}

	protected abstract Function<byte[], IN> deserializer(E engine, Class<IN> type, Consumer<IN> next);

	protected abstract Function<OUT, byte[]> serializer(E engine);

	private String readTypeName(Buffer buffer) {
		int len = buffer.readInt();
		Assert.isTrue(buffer.remaining() > len,
				"Incomplete buffer. Must contain " + len + " bytes, "
						+ "but only " + buffer.remaining() + " were found.");
		byte[] bytes = new byte[len];
		buffer.read(bytes);
		return new String(bytes);
	}

	private Buffer writeTypeName(Class<?> type, byte[] bytes) {
		String typeName = type.getName();
		int len = typeName.length();
		Buffer buffer = new Buffer(4 + len + bytes.length, true);
		return buffer.append(len)
				.append(typeName)
				.append(bytes)
				.flip();

	}

	public Class<IN> readType(Buffer buffer) {
		String typeName = readTypeName(buffer);
		return getType(typeName);
	}

	@SuppressWarnings("unchecked")
	private Class<IN> getType(String name) {
		Class<IN> type = types.get(name);
		if (null == type) {
			try {
				type = (Class<IN>) Class.forName(name);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
			types.put(name, type);
		}
		return type;
	}

	private class DelegateCodec extends Codec<Buffer, IN, OUT> {
		final Function<OUT, byte[]> fn = serializer(engine);

		@Override
		public Function<Buffer, IN> decoder(final Consumer<IN> next) {
			return new Function<Buffer, IN>() {
				@Override
				public IN apply(Buffer buffer) {
					try {
						Class<IN> clazz = readType(buffer);
						byte[] bytes = buffer.asBytes();
						buffer.position(buffer.limit());

						return deserializer(engine, clazz, next).apply(bytes);
					} catch (RuntimeException e) {
						if (log.isErrorEnabled()) {
							log.error("Could not decode " + buffer, e);
						}
						throw e;
					}
				}
			};
		}

		@Override
		public Buffer apply(OUT o) {
			try {
				return writeTypeName(o.getClass(), fn.apply(o));
			} catch (RuntimeException e) {
				if (log.isErrorEnabled()) {
					log.error("Could not encode " + o, e);
				}
				throw e;
			}
		}
	}

}
