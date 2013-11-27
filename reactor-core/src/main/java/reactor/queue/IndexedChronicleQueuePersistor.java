package reactor.queue;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.ChronicleTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.io.encoding.JavaSerializationCodec;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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

	private static final Logger LOG = LoggerFactory.getLogger(IndexedChronicleQueuePersistor.class);

	private final Object     monitor = new Object();
	private final AtomicLong lastId  = new AtomicLong();
	private final AtomicLong size    = new AtomicLong(0);

	private final String                  basePath;
	private final Codec<Buffer, T, T>     codec;
	private final boolean                 deleteOnExit;
	private final IndexedChronicle        data;
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
		this(basePath, new JavaSerializationCodec<T>(), false, true, ChronicleConfig.DEFAULT.clone());
	}

	/**
	 * Create an {@link IndexedChronicleQueuePersistor} based on the given base path, encoder and decoder. Optionally,
	 * passing {@literal false} to {@code clearOnStart} skips clearing the Chronicle on start for appending.
	 *
	 * @param basePath
	 * 		Directory in which to create the Chronicle.
	 * @param codec
	 * 		Codec to turn objects into {@link reactor.io.Buffer Buffers} and visa-versa.
	 * @param clearOnStart
	 * 		Whether or not to clear the Chronicle on start.
	 * @param deleteOnExit
	 * 		Whether or not to delete the Chronicle when the program exits.
	 * @param config
	 * 		ChronicleConfig to use.
	 *
	 * @throws IOException
	 */
	public IndexedChronicleQueuePersistor(@Nonnull String basePath,
																				@Nonnull Codec<Buffer, T, T> codec,
																				boolean clearOnStart,
																				boolean deleteOnExit,
																				@Nonnull ChronicleConfig config) throws IOException {
		this.basePath = basePath;
		this.codec = codec;
		this.deleteOnExit = deleteOnExit;

		if(clearOnStart) {
			for(String name : new String[]{basePath + ".data", basePath + ".index"}) {
				File file = new File(name);
				if(file.exists()) {
					file.delete();
				}
			}
		}

		ChronicleTools.warmup();
		data = new IndexedChronicle(basePath, config);
		lastId.set(data.findTheLastIndex());

		Excerpt ex = data.createExcerpt();
		while(ex.nextIndex()) {
			int len = ex.readInt();
			size.incrementAndGet();
			ex.skip(len);
		}

		offerFun = new ChronicleOfferFunction();
		getFun = new ChronicleGetFunction();
		removeFun = new ChronicleRemoveFunction(data.createTailer());
	}

	/**
	 * Close the underlying chronicles.
	 */
	@Override
	public void close() {
		try {
			data.close();

			if(deleteOnExit) {
				ChronicleTools.deleteOnExit(basePath);
			}
		} catch(IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public long lastId() {
		return lastId.get();
	}

	@Override
	public long size() {
		return size.get();
	}

	@Override
	public boolean hasNext() {
		return removeFun.hasNext();
	}

	@Nonnull
	@Override
	public Function<T, Long> offer() {
		return offerFun;
	}

	@Nonnull
	@Override
	public Function<Long, T> get() {
		return getFun;
	}

	@Nonnull
	@Override
	public Supplier<T> remove() {
		return removeFun;
	}

	@Override
	public Iterator<T> iterator() {
		final ChronicleRemoveFunction fn;
		try {
			fn = new ChronicleRemoveFunction(data.createTailer());
		} catch(IOException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}

		return new Iterator<T>() {
			public boolean hasNext() {
				return fn.hasNext();
			}

			@SuppressWarnings("unchecked")
			@Override
			public T next() {
				return fn.get();
			}

			@Override
			public void remove() {
				throw new IllegalStateException("This Iterator is read-only.");
			}
		};
	}

	@SuppressWarnings("unchecked")
	private T read(ExcerptCommon ex) {
		try {
			int len = ex.readInt();
			ByteBuffer bb = ByteBuffer.allocate(len);
			ex.read(bb);
			bb.flip();
			return codec.decoder(null).apply(new Buffer(bb));
		} finally {
			ex.finish();
		}
	}

	private class ChronicleOfferFunction implements Function<T, Long> {
		private final ExcerptAppender ex;

		private ChronicleOfferFunction() throws IOException {
			this.ex = data.createAppender();
		}

		@Override
		public Long apply(T t) {
			synchronized(monitor) {
				Buffer buff = codec.encoder().apply(t);

				int len = buff.remaining();
				ex.startExcerpt(4 + len);
				ex.writeInt(len);
				ex.write(buff.byteBuffer());
				ex.finish();

				size.incrementAndGet();
				lastId.set(ex.lastWrittenIndex());
			}

			if(LOG.isTraceEnabled()) {
				LOG.trace("Offered {} to Chronicle at index {}, size {}", t, lastId(), size());
			}

			return lastId();
		}
	}

	private class ChronicleGetFunction implements Function<Long, T> {
		private final ExcerptTailer ex;

		private ChronicleGetFunction() throws IOException {
			this.ex = data.createTailer();
		}

		@SuppressWarnings("unchecked")
		@Override
		public T apply(Long id) {
			if(!ex.index(id)) {
				return null;
			}
			return read(ex);
		}
	}

	private class ChronicleRemoveFunction implements Supplier<T> {
		private final ExcerptTailer ex;

		private ChronicleRemoveFunction(ExcerptTailer ex) throws IOException {
			this.ex = ex;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T get() {
			synchronized(monitor) {
				T obj = read(ex);
				size.decrementAndGet();
				return obj;
			}
		}

		public boolean hasNext() {
			return ex.nextIndex();
		}
	}

}
