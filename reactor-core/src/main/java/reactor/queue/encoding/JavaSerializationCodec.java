package reactor.queue.encoding;

import reactor.function.Function;
import reactor.io.Buffer;

import java.io.*;

/**
 * @author Jon Brisbin
 */
public class JavaSerializationCodec<T> implements Codec<T> {

	private final Decoder decoder = new Decoder();
	private final Encoder encoder = new Encoder();

	@Override
	public Function<Buffer, T> decoder() {
		return decoder;
	}

	@Override
	public Function<T, Buffer> encoder() {
		return encoder;
	}

	private class Decoder implements Function<Buffer, T> {
		@SuppressWarnings("unchecked")
		@Override
		public T apply(Buffer buff) {
			if(buff.remaining() <= 0) {
				return null;
			}
			try {
				return (T)new ObjectInputStream(new ByteArrayInputStream(buff.asBytes())).readObject();
			} catch(Exception e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}
	}

	private class Encoder implements Function<T, Buffer> {
		@Override
		public Buffer apply(T t) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				oos.writeObject(t);
				oos.flush();
				oos.close();
			} catch(IOException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}

			return Buffer.wrap(baos.toByteArray());
		}
	}

}
