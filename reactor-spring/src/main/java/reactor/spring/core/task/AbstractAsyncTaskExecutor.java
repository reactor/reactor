package reactor.spring.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncTaskExecutor;
import reactor.core.Environment;
import reactor.event.dispatch.AbstractLifecycleDispatcher;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.timer.Timer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract base class for {@link org.springframework.core.task.AsyncTaskExecutor} implementations that need some basic
 * metadata about how they should be configured.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class AbstractAsyncTaskExecutor implements ScheduledExecutorService,
                                                           AsyncTaskExecutor,
                                                           InitializingBean,
                                                           SmartLifecycle {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final Timer       timer;

	private String name    = getClass().getSimpleName();
	private int    threads = Runtime.getRuntime().availableProcessors();
	private int    backlog = 2048;

	protected AbstractAsyncTaskExecutor(Timer timer) {
		this.timer = timer;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		getDispatcher().awaitAndShutdown();
		callback.run();
	}

	@Override
	public void start() {
	}

	@Override
	public void stop() {
		getDispatcher().shutdown();
	}

	@Override
	public boolean isRunning() {
		return getDispatcher().alive();
	}

	@Override
	public int getPhase() {
		return 0;
	}

	/**
	 * Get the name by which these threads will be known.
	 *
	 * @return name of the threads for this work queue
	 */
	public String getName() {
		return name;
	}

	/**
	 * Set the name by which these threads are known.
	 *
	 * @param name
	 * 		name of the threads for this work queue
	 *
	 * @return {@literal this}
	 */
	public AbstractAsyncTaskExecutor setName(String name) {
		this.name = name;
		return this;
	}

	/**
	 * Get the number of threads being used by this executor.
	 *
	 * @return the number of threads being used
	 */
	public int getThreads() {
		return threads;
	}

	/**
	 * Set the number of threads to use when creating this executor.
	 *
	 * @param threads
	 * 		the number of threads to use
	 *
	 * @return {@literal this}
	 */
	public AbstractAsyncTaskExecutor setThreads(int threads) {
		this.threads = threads;
		return this;
	}

	/**
	 * Get the number of pre-allocated tasks to keep in memory. Correlates directly to the size of the internal {@code
	 * RingBuffer}.
	 *
	 * @return the backlog value
	 */
	public int getBacklog() {
		return backlog;
	}

	/**
	 * Set the number of pre-allocated tasks to keep in memory. Correlates directly to the size of the internal {@code
	 * RingBuffer}.
	 *
	 * @return {@literal this}
	 */
	public AbstractAsyncTaskExecutor setBacklog(int backlog) {
		this.backlog = backlog;
		return this;
	}


	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return getDispatcher().awaitAndShutdown(timeout, unit);
	}

	@Override
	public boolean isTerminated() {
		return !getDispatcher().alive();
	}

	@Override
	public boolean isShutdown() {
		return isTerminated();
	}

	@Override
	public List<Runnable> shutdownNow() {
		getDispatcher().shutdown();
		return Collections.emptyList();
	}

	@Override
	public void shutdown() {
		getDispatcher().shutdown();
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
	                       long timeout,
	                       TimeUnit unit) throws InterruptedException,
	                                             ExecutionException,
	                                             TimeoutException {
		List<FutureTask<T>> submittedTasks = new ArrayList<FutureTask<T>>();
		for(Callable<T> task : tasks) {
			FutureTask<T> ft = new FutureTask<T>(task);
			execute(ft);
			submittedTasks.add(ft);
		}

		T result = null;
		long start = System.currentTimeMillis();
		for(; ; ) {
			for(FutureTask<T> task : submittedTasks) {
				result = task.get(100, TimeUnit.MILLISECONDS);
				if(null != result || task.isDone()) {
					break;
				}
			}
			if(null != result || (System.currentTimeMillis() - start) > TimeUnit.MILLISECONDS.convert(timeout, unit)) {
				break;
			}
		}
		for(FutureTask<T> task : submittedTasks) {
			if(!task.isDone()) {
				task.cancel(true);
			}
		}
		return result;
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
	                                                                       ExecutionException {
		try {
			return invokeAny(tasks, Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch(TimeoutException e) {
			throw new ExecutionException(e.getMessage(), e);
		}
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
	                                     long timeout,
	                                     TimeUnit unit) throws InterruptedException {
		List<Future<T>> submittedTasks = new ArrayList<Future<T>>();
		for(Callable<T> task : tasks) {
			FutureTask<T> ft = new FutureTask<T>(task);
			execute(ft);
			submittedTasks.add(ft);
		}

		T result = null;
		long start = System.currentTimeMillis();
		for(; ; ) {
			boolean allComplete = false;
			for(Future<T> task : submittedTasks) {
				try {
					result = task.get(100, TimeUnit.MILLISECONDS);
				} catch(ExecutionException e) {
					log.error(e.getMessage(), e);
				} catch(TimeoutException e) {
					log.error(e.getMessage(), e);
				}
				if(allComplete = !allComplete && task.isDone()) {
					break;
				}
			}
			if(null != result || (System.currentTimeMillis() - start) > TimeUnit.MILLISECONDS.convert(timeout, unit)) {
				break;
			}
		}
		return submittedTasks;
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return invokeAll(tasks, Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
	}

	@Override
	public void execute(final Runnable task, long startTimeout) {
		timer.submit(
				new Consumer<Long>() {
					@Override
					public void accept(Long now) {
						execute(task);
					}
				},
				startTimeout,
				TimeUnit.MILLISECONDS
		);
	}

	@Override
	public Future<?> submit(Runnable task) {
		final FutureTask<Void> future = new FutureTask<Void>(task, null);
		execute(future);
		return future;
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		final FutureTask<T> future = new FutureTask<T>(task);
		execute(future);
		return future;
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		FutureTask<T> future = new FutureTask<T>(task, result);
		execute(future);
		return future;
	}

	@Override
	public void execute(Runnable task) {
		getDispatcher().execute(task);
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable command,
	                                   long delay,
	                                   TimeUnit unit) {
		long initialDelay = convertToMillis(delay, unit);
		final ScheduledFutureTask<?> future = new ScheduledFutureTask<Object>(command, null, initialDelay);
		timer.submit(new Consumer<Long>() {
			@Override
			public void accept(Long now) {
				execute(future);
			}
		}, initialDelay, TimeUnit.MILLISECONDS);
		return future;
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable,
	                                       long delay,
	                                       TimeUnit unit) {
		long initialDelay = convertToMillis(delay, unit);
		final ScheduledFutureTask<V> future = new ScheduledFutureTask<V>(callable, initialDelay);
		timer.submit(new Consumer<Long>() {
			@Override
			public void accept(Long now) {
				execute(future);
			}
		}, initialDelay, TimeUnit.MILLISECONDS);
		return future;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command,
	                                              long initialDelay,
	                                              long period,
	                                              TimeUnit unit) {
		long initialDelayInMs = convertToMillis(initialDelay, unit);
		long periodInMs = convertToMillis(period, unit);
		final AtomicReference<Registration> registration = new AtomicReference<Registration>();

		final Runnable task = new Runnable() {
			@Override
			public void run() {
				try {
					command.run();
				} catch(Throwable t) {
					log.error(t.getMessage(), t);
					Registration reg;
					if(null != (reg = registration.get())) {
						reg.cancel();
					}
				}
			}
		};

		final Consumer<Long> consumer = new Consumer<Long>() {
			@Override
			public void accept(Long now) {
				execute(task);
			}
		};

		final ScheduledFutureTask<?> future = new ScheduledFutureTask<Object>(task, null, initialDelay);
		registration.set(timer.schedule(consumer, periodInMs, TimeUnit.MILLISECONDS, initialDelayInMs));
		return future;
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command,
	                                                 long initialDelay,
	                                                 long delay,
	                                                 TimeUnit unit) {
		final long initialDelayInMs = convertToMillis(initialDelay, unit);
		final long delayInMs = convertToMillis(initialDelay, unit);
		final ScheduledFutureTask<?> future = new ScheduledFutureTask<Object>(command, null, initialDelayInMs);

		final AtomicReference<Registration> registration = new AtomicReference<Registration>();

		final Consumer<Long> consumer = new Consumer<Long>() {
			final Consumer<Long> self = this;

			@Override
			public void accept(Long now) {
				execute(new Runnable() {
					@Override
					public void run() {
						try {
							future.run();
							timer.submit(self, delayInMs, TimeUnit.MILLISECONDS);
						} catch(Throwable t) {
							log.error(t.getMessage(), t);
							Registration reg;
							if(null != (reg = registration.get())) {
								reg.cancel();
							}
						}
					}
				});
			}
		};

		registration.set(timer.submit(consumer, initialDelayInMs, TimeUnit.MILLISECONDS));
		return future;
	}

	protected abstract AbstractLifecycleDispatcher getDispatcher();

	private static long convertToMillis(long l, TimeUnit timeUnit) {
		if(timeUnit == TimeUnit.MILLISECONDS) {
			return l;
		} else {
			return timeUnit.convert(l, TimeUnit.MILLISECONDS);
		}
	}

	private static class ScheduledFutureTask<T> extends FutureTask<T> implements ScheduledFuture<T> {
		private final long delay;

		private ScheduledFutureTask(Runnable runnable, T result, long delay) {
			super(runnable, result);
			this.delay = delay;
		}

		private ScheduledFutureTask(Callable<T> callable, long delay) {
			super(callable);
			this.delay = delay;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return convertToMillis(delay, unit);
		}

		@Override
		public int compareTo(Delayed d) {
			if(this == d) {
				return 0;
			}
			long diff = getDelay(TimeUnit.MILLISECONDS) - d.getDelay(TimeUnit.MILLISECONDS);
			return (diff == 0 ? 0 : ((diff < 0) ? -1 : 1));
		}
	}
}
