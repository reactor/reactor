package reactor.spring.core.task;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncTaskExecutor;
import reactor.core.Environment;
import reactor.event.dispatch.AbstractLifecycleDispatcher;
import reactor.function.Consumer;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for {@link org.springframework.core.task.AsyncTaskExecutor} implementations that need some basic
 * metadata about how they should be configured.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class AbstractAsyncTaskExecutor implements AsyncTaskExecutor,
                                                           InitializingBean,
                                                           SmartLifecycle {

	private final Environment env;

	private String name    = getClass().getSimpleName();
	private int    threads = Runtime.getRuntime().availableProcessors();
	private int    backlog = 2048;

	protected AbstractAsyncTaskExecutor(Environment env) {
		this.env = env;
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
	public void execute(final Runnable task, long startTimeout) {
		env.getRootTimer().submit(
				new Consumer<Long>() {
					@Override
					public void accept(Long now) {
						getDispatcher().execute(task);
					}
				},
				startTimeout,
				TimeUnit.MILLISECONDS
		);
	}

	@Override
	public Future<?> submit(Runnable task) {
		final FutureTask<Void> future = new FutureTask<Void>(task, null);

		getDispatcher().execute(new Runnable() {
			@Override
			public void run() {
				future.run();
			}
		});

		return future;
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		final FutureTask<T> future = new FutureTask<T>(task);

		getDispatcher().execute(new Runnable() {
			@Override
			public void run() {
				future.run();
			}
		});

		return future;
	}

	@Override
	public void execute(Runnable task) {
		getDispatcher().execute(task);
	}

	protected abstract AbstractLifecycleDispatcher getDispatcher();

}
