package reactor.queue;

import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.io.Buffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link QueuePersistor} implementation that uses a <a href="https://github.com/peter-lawrey/Java-Chronicle">Java
 * Chronicle</a> {@literal IndexedChronicle} to persist items in the queue.
 *
 * @author Jon Brisbin
 * @see <a href="https://github.com/peter-lawrey/Java-Chronicle">Java Chronicle</a>
 */
public class IndexedChronicleQueuePersistor<T> implements QueuePersistor<T> {

	private static final Logger     LOG    = LoggerFactory.getLogger(IndexedChronicleQueuePersistor.class);
	private final        AtomicLong count  = new AtomicLong();
	private final        AtomicLong lastId = new AtomicLong();
	private final String                  basePath;
	private final boolean                 deleteOnExit;
	private final Function<T, Buffer>     encoder;
	private final Function<Buffer, T>     decoder;
	private final ChronicleOfferFunction  offerFun;
	private final ChronicleGetFunction    getFun;
	private final ChronicleRemoveFunction removeFun;

	/**
	 * Create an {@link IndexedChronicleQueuePersistor} based on the given base path.
	 *
	 * @param basePath
	 * 		Directory in which to create the Chronicle.
	 *
	 * @throws IOException
	 */
	public IndexedChronicleQueuePersistor(@Nonnull String basePath) throws IOException {
		this(basePath, null, null, true, false);
	}

	/**
	 * Create an {@link IndexedChronicleQueuePersistor} based on the given base path, encoder and decoder. Optionally,
	 * passing {@literal false} to {@code clearOnStart} skips clearing the Chronicle on start for appending.
	 *
	 * @param basePath
	 * 		Directory in which to create the Chronicle.
	 * @param encoder
	 * 		Encoder to turn objects into a {@link Buffer}.
	 * @param decoder
	 * 		Decoder to turn {@link Buffer Buffers} into an object.
	 * @param clearOnStart
	 * 		Whether or not to clear the Chronicle on start.
	 * @param deleteOnExit
	 * 		Whether or not to delete the Chronicle when the program exits.
	 *
	 * @throws IOException
	 */
	public IndexedChronicleQueuePersistor(@Nonnull String basePath,
	                                      @Nullable Function<T, Buffer> encoder,
	                                      @Nullable Function<Buffer, T> decoder,
	                                      boolean clearOnStart,
	                                      boolean deleteOnExit) throws IOException {
		this.basePath = basePath;
		this.encoder = (null == encoder ? new SerializableEncoder<T>() : encoder);
		this.decoder = (null == decoder ? new SerializableDecoder<T>() : decoder);
		this.deleteOnExit = deleteOnExit;

        if(clearOnStart) {
            for (String name : new String[]{basePath + ".data", basePath + ".index"}) {
                File file = new File(name);
                if(file.exists()) {
                    file.delete();
                }
            }
        }

		this.offerFun = new ChronicleOfferFunction(new IndexedChronicle(basePath));
        this.getFun = new ChronicleGetFunction(new IndexedChronicle(basePath));
        this.removeFun = new ChronicleRemoveFunction(new IndexedChronicle(basePath));
	}

	/**
	 * Close the underlying chronicles.
	 */
	@Override
	public void close() {
        try {
            offerFun.chronicle.close();
            getFun.chronicle.close();
            removeFun.chronicle.close();
            if(deleteOnExit) {
                ChronicleTools.deleteOnExit(basePath);
            }
        } catch(IOException e) {
            //TODO: what to do ?
            LOG.warn("IOException",e);
            throw new IllegalStateException(e.getMessage(), e);
        }
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
		return new Iterator<T>() {
			private final ChronicleRemoveFunction iterFun;

			{
				try {
					iterFun = new ChronicleRemoveFunction(new IndexedChronicle(basePath));
				} catch(IOException e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}
			}

			public boolean hasNext() {
                //TODO: may lead to resource content?
                long index = iterFun.ex.index();
				boolean has = iterFun.ex.nextIndex();
                iterFun.ex.index(index);

                return has;
			}

			@Override public T next() {
				return iterFun.get();
			}

			@Override public void remove() {
			}
		};
	}

	private T read(Excerpt ex, long index) {
		if(!ex.index(index)) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Could not read requested index [" + index + "]");
			}
			return null;
		}

		int len = -1;
		try {
			len = ex.readInt();
			byte[] bytes = new byte[len];
			ex.read(bytes);
			ex.finish();

			return decoder.apply(Buffer.wrap(bytes));
		} catch(Throwable t) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Asked to read: " + len + "b from index " + ex.index());
				LOG.debug(t.getMessage(), t);
			}
			return null;
		}
	}

	private class ChronicleOfferFunction implements Function<T, Long> {
		private final IndexedChronicle chronicle;
		private final ExcerptAppender ex;

		private ChronicleOfferFunction(IndexedChronicle chronicle) throws IOException {
			this.chronicle = chronicle;
			this.ex = chronicle.createAppender();
		}

		@Override public Long apply(T t) {
			Buffer buff = encoder.apply(t);

			ex.startExcerpt(4 + buff.remaining());
			ex.writeInt(buff.remaining());
			ex.write(buff.asBytes());

			count.incrementAndGet();
			lastId.set(ex.index());

			ex.finish();

			if(LOG.isTraceEnabled()) {
				LOG.trace("Offered " + t + " to " + chronicle + " at index " + lastId());
			}

			return lastId();
		}
	}

	private class ChronicleGetFunction implements Function<Long, T> {
		private final IndexedChronicle chronicle;
		private final Excerpt ex;

		private ChronicleGetFunction(IndexedChronicle chronicle) throws IOException {
			this.chronicle = chronicle;
			this.ex = chronicle.createExcerpt();
		}

		@Override public T apply(Long id) {
			return read(ex, id);
		}
	}

	private class ChronicleRemoveFunction implements Supplier<T> {
		private final IndexedChronicle chronicle;
		private final Excerpt          ex;

		private ChronicleRemoveFunction(IndexedChronicle chronicle) throws IOException {
			this.chronicle = chronicle;
			this.ex = chronicle.createExcerpt();
		}

		@Override public T get() {
			if(!ex.nextIndex()) {
				return null;
			}

			T obj = read(ex, ex.index());
			count.decrementAndGet();
			return obj;
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
