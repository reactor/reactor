package reactor.core.fork;

import com.gs.collections.api.list.ImmutableList;
import com.gs.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.core.composable.spec.Streams;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Function;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple {@link java.util.concurrent.Executor} backed fork/join pool that will coalesce results from asynchronous tasks
 * and publish them into a {@link reactor.core.composable.Promise} or a {@link reactor.core.composable.Stream} depending
 * on whether you intend to {@code fork()} an execution or {@code join()} them together. tasks
 *
 * @author Jon Brisbin
 */
public class ForkJoinPool {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Environment env;
	private final Dispatcher  dispatcher;
	private final Executor    executor;

	public ForkJoinPool(Environment env) {
		this(env, env.getDefaultDispatcher(), env.getDispatcher(Environment.THREAD_POOL));
	}

	public ForkJoinPool(Environment env,
	                    Dispatcher dispatcher,
	                    Executor executor) {
		this.env = env;
		this.dispatcher = dispatcher;
		this.executor = executor;
	}

	/**
	 * Asynchronously submit the given tasks, one submit per task, to the configured {@link java.util.concurrent.Executor}
	 * and collecting the results in a {@link java.util.List} that will be the fulfillment of the {@link
	 * reactor.core.composable.Promise} returned from {@link ForkJoinTask#compose()}.
	 *
	 * @param tasks
	 * 		asynchronous tasks to execute
	 * @param <V>
	 * 		type of task result
	 *
	 * @return fork/join task
	 */
	public <V> ForkJoinTask<ImmutableList<V>, Promise<ImmutableList<V>>> join(final Function<?, V>... tasks) {
		return join(Arrays.asList(tasks));
	}

	/**
	 * Asynchronously submit the given tasks, one submit per task, to the configured {@link java.util.concurrent.Executor}
	 * and collecting the results in a {@link java.util.List} that will be the fulfillment of the {@link
	 * reactor.core.composable.Promise} returned from {@link ForkJoinTask#compose()}.
	 *
	 * @param tasks
	 * 		asynchronous tasks to execute
	 * @param <V>
	 * 		type of task result
	 *
	 * @return fork/join task
	 */
	public <V> ForkJoinTask<ImmutableList<V>, Promise<ImmutableList<V>>> join(final Collection<Function<?, V>> tasks) {
		final Deferred<ImmutableList<V>, Promise<ImmutableList<V>>> d
				= Promises.defer(env, dispatcher);
		final ForkJoinTask<ImmutableList<V>, Promise<ImmutableList<V>>> t
				= new ForkJoinTask<ImmutableList<V>, Promise<ImmutableList<V>>>(executor, d);

		final AtomicInteger count = new AtomicInteger(tasks.size());
		final FastList<V> results = FastList.newList();

		for (final Function fn : tasks) {
			t.add(new Function<Object, ImmutableList<V>>() {
				@SuppressWarnings("unchecked")
				@Override
				public ImmutableList<V> apply(Object o) {
					try {
						V result = (V) fn.apply(o);
						if (null != result) {
							synchronized (results) {
								results.add(result);
							}
						}
					} finally {
						if (count.decrementAndGet() == 0) {
							d.accept(results.toImmutable());
						}
					}
					return null;
				}
			});
		}

		return t;
	}

	/**
	 * Asynchronously execute tasks added to the returned {@link reactor.core.fork.ForkJoinTask} and publish the non-null
	 * results, one per task, to the {@link Stream} returned from {@link ForkJoinTask#compose()}.
	 *
	 * @param <V>
	 * 		type of task result
	 *
	 * @return fork/join task
	 */
	public <V> ForkJoinTask<V, Stream<V>> fork() {
		Deferred<V, Stream<V>> d = Streams.defer(env, dispatcher);
		return new ForkJoinTask<V, Stream<V>>(executor, d);
	}

}
