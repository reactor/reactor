package reactor.core.fork;

import reactor.core.Environment;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.core.composable.spec.Streams;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Simple {@link java.util.concurrent.Executor} backed fork/join pool that will coalesce results from asynchronous tasks
 * and publish them into a {@link reactor.core.composable.Promise} or a {@link reactor.core.composable.Stream} depending
 * on whether you intend to {@code fork()} an execution or {@code join()} them together. tasks
 *
 * @author Jon Brisbin
 */
public class ForkJoinPool {

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
	public <V> ForkJoinTask<List<V>, Promise<List<V>>> join(final Function<?, V>... tasks) {
		Deferred<List<V>, Promise<List<V>>> d = Promises.defer(env, dispatcher);
		ForkJoinTask<List<V>, Promise<List<V>>> t = new ForkJoinTask<List<V>, Promise<List<V>>>(executor, d);
		t.add(new Function<Object, List<V>>() {
			@SuppressWarnings("unchecked")
			@Override
			public List<V> apply(Object o) {
				List<V> results = new ArrayList<V>();
				for (Function task : tasks) {
					V result = (V) task.apply(o);
					if (null != result) {
						results.add(result);
					}
				}
				return results;
			}
		});
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
