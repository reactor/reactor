package reactor.queue.encoding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import reactor.function.Function;
import reactor.io.Buffer;

import java.io.IOException;

/**
 * @author Jon Brisbin
 */
public class JsonCodec<T> implements Codec<T> {

	private final Class<T> type;
	private final Function<Buffer, T> decoder = new Decoder();
	private final Function<T, Buffer> encoder = new Encoder();
	private final ObjectMapper mapper;

	public JsonCodec(Class<T> type) {
		this.type = type;
		this.mapper = new ObjectMapper();
		this.mapper.registerModule(new AfterburnerModule());
		this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
		this.mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
	}

	public JsonCodec(Class<T> type, ObjectMapper mapper) {
		this.type = type;
		this.mapper = mapper;
	}

	@Override
	public Function<Buffer, T> decoder() {
		return decoder;
	}

	@Override
	public Function<T, Buffer> encoder() {
		return encoder;
	}

	private class Decoder implements Function<Buffer, T> {
		@Override
		public T apply(Buffer buffer) {
			try {
				return mapper.readValue(buffer.asBytes(), type);
			} catch(IOException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}
	}

	private class Encoder implements Function<T, Buffer> {
		@Override
		public Buffer apply(T t) {
			try {
				return Buffer.wrap(mapper.writeValueAsBytes(t));
			} catch(JsonProcessingException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}
	}

}
