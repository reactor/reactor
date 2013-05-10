package reactor.fn.dispatch;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.fn.Consumer;
import reactor.fn.ConsumerInvoker;
import reactor.fn.ConverterAwareConsumerInvoker;
import reactor.fn.Event;
import reactor.fn.Lifecycle;
import reactor.fn.Registration;

/**
 * A {@code Dispatcher} that uses a {@link ThreadPoolExecutor} with an unbounded queue to execute {@link Task Tasks}.
 *
 * @author Andy Wilkinson
 */
public final class ThreadPoolExecutorDispatcher implements Dispatcher {

	private static final Logger LOG = LoggerFactory.getLogger(SynchronousDispatcher.class);

	private static final AtomicInteger THREAD_COUNT = new AtomicInteger();

	private volatile ConsumerInvoker invoker  = new ConverterAwareConsumerInvoker();

	private final ExecutorService executor;

	public ThreadPoolExecutorDispatcher(int poolSize) {
		executor = Executors.newFixedThreadPool(poolSize, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, "thread-pool-executor-" + THREAD_COUNT.incrementAndGet());
			}
		});
	}

	@Override
	public Lifecycle destroy() {
		return this;
	}

	@Override
	public Lifecycle stop() {
		executor.shutdown();
		return this;
	}

	@Override
	public Lifecycle start() {
		return this;
	}

	@Override
	public boolean isAlive() {
		return executor.isShutdown();
	}

	@Override
	public ConsumerInvoker getConsumerInvoker() {
		return this.invoker;
	}

	@Override
	public Dispatcher setConsumerInvoker(ConsumerInvoker consumerInvoker) {
		this.invoker = invoker;
		return this;
	}

	@Override
	public <T> Task<T> nextTask() {
		return new ExecutorTask<T>();
	}

	private class ExecutorTask<T> extends Task<T> {
		@Override
		public void submit() {
			executor.execute(new Runnable() {
				public void run() {
					try {
						for (Registration<? extends Consumer<? extends Event<?>>> reg : getConsumerRegistry().select(getSelector())) {
							if (reg.isCancelled() || reg.isPaused()) {
								continue;
							}
							reg.getSelector().setHeaders(getSelector(), getEvent());
							invoker.invoke(reg.getObject(), getConverter(), Void.TYPE, getEvent());
							if (reg.isCancelAfterUse()) {
								reg.cancel();
							}
						}
						if (null != getCompletionConsumer()) {
							invoker.invoke(getCompletionConsumer(), getConverter(), Void.TYPE, getEvent());
						}
					} catch (Throwable x) {
						LOG.error(x.getMessage(), x);
						if (null != getErrorConsumer()) {
							getErrorConsumer().accept(x);
						}
					}
				}
			});
		}
	}
}
