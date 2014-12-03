/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.core.fork;

import com.gs.collections.api.list.ImmutableList;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Function;
import reactor.rx.Streams;
import reactor.rx.stream.HotStream;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple {@link java.util.concurrent.Executor} backed fork/join pool that will coalesce results from asynchronous tasks
 * and publish them into a {@link reactor.rx.Promise} or a {@link reactor.rx.Stream} depending
 * on whether you intend to {@code fork()} an execution or {@code join()} them together. tasks
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
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
	 * reactor.rx.Promise} returned from {@link ForkJoinTask#compose()}.
	 *
	 * @param tasks
	 * 		asynchronous tasks to execute
	 * @param <V>
	 * 		type of task result
	 *
	 * @return fork/join task
	 */
	public <V> ForkJoinTask<ImmutableList<V>, HotStream<ImmutableList<V>>> join(final Function<?, V>... tasks) {
		return join(Arrays.asList(tasks));
	}

	/**
	 * Asynchronously submit the given tasks, one submit per task, to the configured {@link java.util.concurrent.Executor}
	 * and collecting the results in a {@link java.util.List} that will be the fulfillment of the {@link
	 * reactor.rx.Promise} returned from {@link ForkJoinTask#compose()}.
	 *
	 * @param tasks
	 * 		asynchronous tasks to execute
	 * @param <V>
	 * 		type of task result
	 *
	 * @return fork/join task
	 */
	public <V> ForkJoinTask<ImmutableList<V>, HotStream<ImmutableList<V>>> join(final Collection<Function<?, V>> tasks) {
		final HotStream<ImmutableList<V>> d
				= Streams.defer(env, dispatcher);
		final ForkJoinTask<ImmutableList<V>, HotStream<ImmutableList<V>>> t
				= new ForkJoinTask<ImmutableList<V>, HotStream<ImmutableList<V>>>(executor, d);

		final AtomicInteger count = new AtomicInteger(tasks.size());
		final MutableList<V> results = FastList.newList();

		for (final Function fn : tasks) {
			t.add(new Function<Object, ImmutableList<V>>() {
				@SuppressWarnings("unchecked")
				@Override
				public ImmutableList<V> apply(Object o) {
					try {
						V result = (V) fn.apply(o);
						synchronized (results) {
							results.add(result);
						}
					} finally {
						if (count.decrementAndGet() == 0) {
							d.broadcastNext(results.toImmutable());
							d.broadcastComplete();
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
	 * results, one per task, to the {@link reactor.rx.Stream} returned from {@link ForkJoinTask#compose()}.
	 *
	 * @param <V>
	 * 		type of task result
	 *
	 * @return fork/join task
	 */
	public <V> ForkJoinTask<V, HotStream<V>> fork() {
		HotStream<V> d = Streams.defer(env, dispatcher);
		return new ForkJoinTask<V, HotStream<V>>(executor, d);
	}

}
