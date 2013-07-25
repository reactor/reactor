package reactor.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.higherfrequencytrading.chronicle.Excerpt;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public class IndexedChronicleQueuePersistor<T> implements QueuePersistor<T> {

	private final AtomicLong count  = new AtomicLong();
	private final AtomicLong lastId = new AtomicLong();
	private final Function<T, Buffer>     encoder;
	private final Function<Buffer, T>     decoder;
	private final ChronicleOfferFunction  offerFun;
	private final ChronicleGetFunction    getFun;
	private final ChronicleRemoveFunction removeFun;


	public IndexedChronicleQueuePersistor(@Nonnull String basePath) throws IOException {
		this(basePath, null, null, true);
	}

	public IndexedChronicleQueuePersistor(@Nonnull String basePath,
	                                      @Nullable Function<T, Buffer> encoder,
	                                      @Nullable Function<Buffer, T> decoder,
	                                      boolean clearOnStart) throws IOException {
		this.encoder = (null == encoder ? new SerializableEncoder<T>() : encoder);
		this.decoder = (null == decoder ? new SerializableDecoder<T>() : decoder);

		this.offerFun = new ChronicleOfferFunction(new IndexedChronicle(basePath));
		if(clearOnStart) {
			this.offerFun.chronicle.clear();
		}
		this.getFun = new ChronicleGetFunction(new IndexedChronicle(basePath));
		if(clearOnStart) {
			this.getFun.chronicle.clear();
		}
		this.removeFun = new ChronicleRemoveFunction(new IndexedChronicle(basePath));
		if(clearOnStart) {
			this.removeFun.chronicle.clear();
		}
	}

	public void close() {
		offerFun.chronicle.close();
		getFun.chronicle.close();
		removeFun.chronicle.close();
	}

	@Override public long lastId() {
		return lastId.get();
	}

	@Override public long size() {
		return count.get();
	}

	@Nonnull @Override public Function<T, Long> offer() {
		return offerFun;
	}

	@Nonnull @Override public Function<Long, T> get() {
		return getFun;
	}

	@Nonnull @Override public Supplier<T> remove() {
		return removeFun;
	}

	@Override public Iterator<T> iterator() {
		return null;
	}

	private class ChronicleOfferFunction implements Function<T, Long> {
		private final IndexedChronicle chronicle;
		private final Excerpt          ex;

		private ChronicleOfferFunction(IndexedChronicle chronicle) {
			this.chronicle = chronicle;
			this.ex = chronicle.createExcerpt();
		}

		@Override public Long apply(T t) {
			Buffer buff = encoder.apply(t);

			ex.startExcerpt(4 + buff.remaining());
			ex.writeInt(buff.remaining());
			ex.write(buff.asBytes());
			ex.finish();

			count.incrementAndGet();
			lastId.set(ex.index());

			return lastId();
		}
	}

	private class ChronicleGetFunction implements Function<Long, T> {
		private final IndexedChronicle chronicle;
		private final Excerpt          ex;

		private ChronicleGetFunction(IndexedChronicle chronicle) {
			this.chronicle = chronicle;
			this.ex = chronicle.createExcerpt();
		}

		@Override public T apply(Long id) {
			while(!ex.index(id)) {
				Thread.yield();
			}

			int len = ex.readInt();
			byte[] bytes = new byte[len];
			ex.read(bytes);
			ex.finish();

			return decoder.apply(Buffer.wrap(bytes));
		}
	}

	private class ChronicleRemoveFunction implements Supplier<T> {
		private final IndexedChronicle chronicle;
		private final Excerpt          ex;

		private ChronicleRemoveFunction(IndexedChronicle chronicle) {
			this.chronicle = chronicle;
			this.ex = chronicle.createExcerpt();
		}

		@Override public T get() {
			if(!ex.nextIndex()) {
				return null;
			}

			int len = ex.readInt();
			byte[] bytes = new byte[len];
			ex.read(bytes);
			ex.finish();

			count.decrementAndGet();

			return decoder.apply(Buffer.wrap(bytes));
		}
	}

	private static class SerializableEncoder<T> implements Function<T, Buffer> {
		@Override public Buffer apply(T t) {
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

	private static class SerializableDecoder<T> implements Function<Buffer, T> {
		@SuppressWarnings("unchecked")
		@Override public T apply(Buffer buff) {
			try {
				ByteArrayInputStream bais = new ByteArrayInputStream(buff.asBytes());
				ObjectInputStream ois = new ObjectInputStream(bais);
				return (T)ois.readObject();
			} catch(IOException e) {
				throw new IllegalStateException(e.getMessage(), e);
			} catch(ClassNotFoundException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}
	}

}
