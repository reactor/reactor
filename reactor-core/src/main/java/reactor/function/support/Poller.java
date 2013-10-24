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
 * A {@code Poller} funnels non-null values retrieved from a {@link reactor.function.Supplier} to the given {@link
 * reactor.function.Consumer} using a separate thread created for each instance of a {@code Poller}.
 *
 * @author Jon Brisbin
 */
public class Poller<T> {

	private static final Logger LOG = LoggerFactory.getLogger(Poller.class);

	private final ExecutorService threadPool = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory("pipe"));
	private final Runnable        reader     = new Runnable() {
		@Override
		public void run() {
			while(active) {
				T obj = supplier.get();
				if(null != obj) {
					consumer.accept(obj);
				} else {
					try {
						Thread.sleep(100);
					} catch(InterruptedException e) {
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

	/**
	 * Poll the given {@link reactor.function.Supplier} for non-null values and pass them to the given {@link
	 * reactor.function.Consumer}.
	 *
	 * @param supplier
	 * 		the {@link reactor.function.Supplier} to poll
	 * @param consumer
	 * 		the {@link reactor.function.Consumer} to pass values to
	 */
	public Poller(Supplier<T> supplier, Consumer<T> consumer) {
		this(supplier, consumer, false);
	}

	/**
	 * Poll the given {@link reactor.function.Supplier} for non-null values and pass them to the given {@link
	 * reactor.function.Consumer}. If {@code paused} is {@code false}, then don't start polling values from the supplier
	 * until {@link #resume()} is invoked.
	 *
	 * @param supplier
	 * 		the {@link reactor.function.Supplier} to poll
	 * @param consumer
	 * 		the {@link reactor.function.Consumer} to pass values to
	 * @param paused
	 * 		whether to start this {@code Poller} in a paused state or not
	 */
	public Poller(Supplier<T> supplier, Consumer<T> consumer, boolean paused) {
		this.supplier = supplier;
		this.consumer = consumer;
		if(!paused) {
			resume();
		}
	}

	/**
	 * Resume polling values from the {@link reactor.function.Supplier}.
	 *
	 * @return {@code this}
	 */
	public synchronized Poller<T> resume() {
		if(threadPool.isShutdown() || threadPool.isTerminated()) {
			throw new IllegalStateException("Poller has been shutdown.");
		}
		if(null != readerFuture) {
			// already started
			return this;
		}
		active = true;
		readerFuture = threadPool.submit(reader);
		if(LOG.isDebugEnabled()) {
			LOG.debug("Started pipe from " + supplier + " to " + consumer);
		}
		return this;
	}

	/**
	 * Pausing polling for values from this {@link reactor.function.Supplier}.
	 *
	 * @return {@code this}
	 */
	public synchronized Poller<T> pause() {
		active = false;
		if(null != readerFuture) {
			readerFuture.cancel(true);
			readerFuture = null;
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("Stopped pipe from " + supplier + " to " + consumer);
		}
		return this;
	}

	/**
	 * Shutdown this {@code Poller} and stop the internal thread from polling the supplier.
	 */
	public void shutdown() {
		threadPool.shutdown();
	}

}
