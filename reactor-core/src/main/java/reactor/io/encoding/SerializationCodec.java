package reactor.io.encoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.Buffer;
import reactor.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Jon Brisbin
 */
public abstract class SerializationCodec<E> implements Codec<Buffer, Object, Object> {

	private final Logger                log   = LoggerFactory.getLogger(getClass());
	private final Map<String, Class<?>> types = new ConcurrentHashMap<String, Class<?>>();
	private final E engine;

	protected SerializationCodec(E engine) {
		this.engine = engine;
	}

	@Override
	public Function<Buffer, Object> decoder(Consumer<Object> next) {
		return new LengthFieldCodec<Object, Object>(new DelegateCodec()).decoder(next);
	}

	@Override
	public Function<Object, Buffer> encoder() {
		return new LengthFieldCodec<Object, Object>(new DelegateCodec()).encoder();
	}

	protected E getEngine() {
		return engine;
	}

	protected abstract Function<byte[], Object> deserializer(E engine, Class<?> type, Consumer<Object> next);

	protected abstract Function<Object, byte[]> serializer(E engine);

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

	public Class<?> readType(Buffer buffer) {
		String typeName = readTypeName(buffer);
		return getType(typeName);
	}

	private Class<?> getType(String name) {
		Class<?> type = types.get(name);
		if(null == type) {
			try {
				type = Class.forName(name);
			} catch(ClassNotFoundException e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
			types.put(name, type);
		}
		return type;
	}

	private class DelegateCodec implements Codec<Buffer, Object, Object> {
		@Override
		public Function<Buffer, Object> decoder(final Consumer<Object> next) {
			return new Function<Buffer, Object>() {
				@Override
				public Object apply(Buffer buffer) {
					try {
						return deserializer(engine, readType(buffer), next).apply(buffer.asBytes());
					} catch(RuntimeException e) {
						if(log.isErrorEnabled()) {
							log.error("Could not decode " + buffer, e);
						}
						throw e;
					}
				}
			};
		}

		@Override
		public Function<Object, Buffer> encoder() {
			final Function<Object, byte[]> fn = serializer(engine);
			return new Function<Object, Buffer>() {
				@Override
				public Buffer apply(Object o) {
					try {
						return writeTypeName(o.getClass(), fn.apply(o));
					} catch(RuntimeException e) {
						if(log.isErrorEnabled()) {
							log.error("Could not encode " + o, e);
						}
						throw e;
					}
				}
			};
		}
	}

}
