package reactor.io.encoding;

import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.Buffer;

import java.io.*;

/**
 * {@code Codec} to transform Java objects into {@link reactor.io.Buffer Buffers} and visa-versa.
 *
 * @author Jon Brisbin
 */
public class JavaSerializationCodec<T> implements Codec<Buffer, T, T> {

	private final Encoder encoder = new Encoder();

	@Override
	public Function<Buffer, T> decoder(Consumer<T> next) {
		return new Decoder(next);
	}

	@Override
	public Function<T, Buffer> encoder() {
		return encoder;
	}

	private class Decoder implements Function<Buffer, T> {
		private final Consumer<T> next;

		private Decoder(Consumer<T> next) {
			this.next = next;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T apply(Buffer buff) {
			if(buff.remaining() <= 0) {
				return null;
			}
			try {
				T obj = (T)new ObjectInputStream(new ByteArrayInputStream(buff.asBytes())).readObject();
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
