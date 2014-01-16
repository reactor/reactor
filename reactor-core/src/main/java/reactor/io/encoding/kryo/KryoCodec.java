package reactor.io.encoding.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.UnsafeMemoryInput;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.Buffer;
import reactor.io.encoding.SerializationCodec;

/**
 * @author Jon Brisbin
 */
public class KryoCodec extends SerializationCodec<Kryo> {

	public KryoCodec(Kryo engine) {
		super(engine);
	}

	@Override
	protected Function<byte[], Object> deserializer(final Kryo engine,
	                                                final Class<?> type,
	                                                final Consumer<Object> next) {
		return new Function<byte[], Object>() {
			@Override
			public Object apply(byte[] bytes) {
				Object obj = engine.readObject(new UnsafeMemoryInput(bytes), type);
				if(null != next) {
					next.accept(obj);
					return null;
				} else {
					return obj;
				}
			}
		};
	}

	@Override
	protected Function<Object, byte[]> serializer(final Kryo engine) {
		return new Function<Object, byte[]>() {
			@Override
			public byte[] apply(Object o) {
				UnsafeMemoryOutput out = new UnsafeMemoryOutput(Buffer.SMALL_BUFFER_SIZE, Buffer.MAX_BUFFER_SIZE);
				engine.writeObject(out, o);
				out.flush();
				return out.toBytes();
			}
		};
	}

}
