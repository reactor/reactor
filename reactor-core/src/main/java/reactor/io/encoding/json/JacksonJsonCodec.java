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
