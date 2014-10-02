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

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.rx.action.Action;

import java.util.concurrent.Executor;

/**
 * Represents a collection of asynchronous tasks that will be executed once per {@link #submit()}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ForkJoinTask<T, C extends Action<T,T>> implements Consumer<Object> {

	private final MultiReaderFastList<Function<?, ?>> tasks = MultiReaderFastList.newList();

	private final Executor executor;
	private final C        deferred;

	ForkJoinTask(Executor executor, C deferred) {
		this.executor = executor;
		this.deferred = deferred;
	}

	/**
	 * Get the {@link com.gs.collections.api.list.MutableList} of tasks that will be submitted when {@link
	 * #submit(Object)} is called.
	 *
	 * @return
	 */
	public MutableList<Function<?, ?>> getTasks() {
		return tasks;
	}

	/**
	 * Add a task to the collection of tasks to be executed.
	 *
	 * @param fn  task to submit
	 * @param <V> type of result
	 * @return {@code this}
	 */
	public <V> ForkJoinTask<T, C> add(Function<V, T> fn) {
		tasks.add(fn);
		return this;
	}

	/**
	 * Compose actions against the asynchronous results.
	 *
	 * @return {@link reactor.rx.Promise} or a {@link reactor.rx.Stream} depending on the
	 * implementation.
	 */
	public C compose() {
		return deferred;
	}

	/**
	 * Submit all configured tasks, possibly in parallel (depending on the configuration of the {@link
	 * java.util.concurrent.Executor} in use), passing {@code null} as an input parameter.
	 */
	public void submit() {
		accept(null);
	}

	/**
	 * Submit all configured tasks, possibly in parallel (depending on the configuration of the {@link
	 * java.util.concurrent.Executor} in use), passing {@code arg} as an input parameter.
	 *
	 * @param arg input parameter
	 * @param <V> type of input parameter
	 */
	public <V> void submit(V arg) {
		accept(arg);
	}

	@Override
	public void accept(final Object arg) {
		tasks.forEach(new CheckedProcedure<Function>() {
			@SuppressWarnings("unchecked")
			@Override
			public void safeValue(final Function fn) throws Exception {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							Object result = fn.apply(arg);
							if (null != result) {
								deferred.broadcastNext((T) result);
							}
						} catch (Exception e) {
							deferred.broadcastError(e);
						}
					}
				});
			}
		});
	}

}
