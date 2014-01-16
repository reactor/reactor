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
 * Abstract base class for {@code Codec Codecs} that perform serialization of objects. Optionally handles writing class
 * names so that an object that is serialized can be properly instantiated with full type information on the other end.
 *
 * @author Jon Brisbin
 */
public abstract class SerializationCodec<E, IN, OUT> implements Codec<Buffer, IN, OUT> {

	private final Logger                 log   = LoggerFactory.getLogger(getClass());
	private final Map<String, Class<IN>> types = new ConcurrentHashMap<String, Class<IN>>();
	private final E       engine;
	private final boolean lengthFieldFraming;

	/**
	 * Create a {@code SerializationCodec} using the given engine and specifying whether or not to prepend a length field
	 * to frame the message.
	 *
	 * @param engine
	 * 		the engine which will perform the serialization
	 * @param lengthFieldFraming
	 * 		{@code true} to prepend a length field, or {@code false} to skip
	 */
	protected SerializationCodec(E engine, boolean lengthFieldFraming) {
		this.engine = engine;
		this.lengthFieldFraming = lengthFieldFraming;
	}

	@Override
	public Function<Buffer, IN> decoder(Consumer<IN> next) {
		if(lengthFieldFraming) {
			return new LengthFieldCodec<IN, OUT>(new DelegateCodec()).decoder(next);
		} else {
			return new DelegateCodec().decoder(next);
		}
	}

	@Override
	public Function<OUT, Buffer> encoder() {
		if(lengthFieldFraming) {
			return new LengthFieldCodec<IN, OUT>(new DelegateCodec()).encoder();
		} else {
			return new DelegateCodec().encoder();
		}
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
		if(null == type) {
			try {
				type = (Class<IN>)Class.forName(name);
			} catch(ClassNotFoundException e) {
				throw new IllegalArgumentException(e.getMessage(), e);
			}
			types.put(name, type);
		}
		return type;
	}

	private class DelegateCodec implements Codec<Buffer, IN, OUT> {
		@Override
		public Function<Buffer, IN> decoder(final Consumer<IN> next) {
			return new Function<Buffer, IN>() {
				@Override
				public IN apply(Buffer buffer) {
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
		public Function<OUT, Buffer> encoder() {
			final Function<OUT, byte[]> fn = serializer(engine);
			return new Function<OUT, Buffer>() {
				@Override
				public Buffer apply(OUT o) {
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
