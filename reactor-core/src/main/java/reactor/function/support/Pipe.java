package reactor.function.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Jon Brisbin
 */
public class Pipe<T> {

	private static final Logger LOG = LoggerFactory.getLogger(Pipe.class);

	private final ExecutorService threadPool = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory("pipe"));
	private final Runnable        reader     = new Runnable() {
		@Override
		public void run() {
			while (active) {
				T obj = supplier.get();
				if (null != obj) {
					consumer.accept(obj);
				} else {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		}
	};
	private final    Supplier<T> supplier;
	private final    Consumer<T> consumer;
	private volatile boolean     active;
	private volatile Future<?>   readerFuture;

	public Pipe(Supplier<T> supplier, Consumer<T> consumer) {
		this(supplier, consumer, false);
	}

	public Pipe(Supplier<T> supplier, Consumer<T> consumer, boolean paused) {
		this.supplier = supplier;
		this.consumer = consumer;
		if (!paused) {
			resume();
		}
	}

	public synchronized Pipe<T> resume() {
		if (threadPool.isShutdown() || threadPool.isTerminated()) {
			throw new IllegalStateException("Pipe has been shutdown.");
		}
		if (null != readerFuture) {
			// already started
			return this;
		}
		active = true;
		readerFuture = threadPool.submit(reader);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Started pipe from " + supplier + " to " + consumer);
		}
		return this;
	}

	public synchronized Pipe<T> pause() {
		active = false;
		if (null != readerFuture) {
			readerFuture.cancel(true);
			readerFuture = null;
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Stopped pipe from " + supplier + " to " + consumer);
		}
		return this;
	}

	public void shutdown() {
		threadPool.shutdown();
	}

}
