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
public class KryoCodec<IN, OUT> extends SerializationCodec<Kryo, IN, OUT> {

	public KryoCodec() {
		super(new Kryo(), true);
	}

	public KryoCodec(Kryo engine, boolean lengthFieldFraming) {
		super(engine, lengthFieldFraming);
	}

	@Override
	protected Function<byte[], IN> deserializer(final Kryo engine,
	                                            final Class<IN> type,
	                                            final Consumer<IN> next) {
		return new Function<byte[], IN>() {
			@Override
			public IN apply(byte[] bytes) {
				IN obj = engine.readObject(new UnsafeMemoryInput(bytes), type);
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
	protected Function<OUT, byte[]> serializer(final Kryo engine) {
		return new Function<OUT, byte[]>() {
			@Override
			public byte[] apply(OUT o) {
				UnsafeMemoryOutput out = new UnsafeMemoryOutput(Buffer.SMALL_BUFFER_SIZE, Buffer.MAX_BUFFER_SIZE);
				engine.writeObject(out, o);
				out.flush();
				return out.toBytes();
			}
		};
	}

}
