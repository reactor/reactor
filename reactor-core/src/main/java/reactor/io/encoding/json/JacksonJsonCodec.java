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
public class JacksonJsonCodec extends SerializationCodec<ObjectMapper> {

	public JacksonJsonCodec(ObjectMapper engine) {
		super(engine);
	}

	@Override
	protected Function<byte[], Object> deserializer(final ObjectMapper engine,
	                                                final Class<?> type,
	                                                final Consumer<Object> next) {
		return new Function<byte[], Object>() {
			@Override
			public Object apply(byte[] bytes) {
				try {
					Object o = engine.readValue(bytes, type);
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
	protected Function<Object, byte[]> serializer(final ObjectMapper engine) {
		return new Function<Object, byte[]>() {
			@Override
			public byte[] apply(Object o) {
				try {
					return engine.writeValueAsBytes(o);
				} catch(JsonProcessingException e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}
			}
		};
	}

}
