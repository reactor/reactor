/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.composable.spec;

import reactor.core.Environment;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Supplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Helper methods for creating {@link Deferred} instances, backed by a {@link Promise}.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public abstract class Promises {

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Promise<T>> defer(Environment env) {
		return defer(env, env.getDefaultDispatcher());
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the name of the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Promise<T>> defer(Environment env, String dispatcher) {
		return defer(env, env.getDispatcher(dispatcher));
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Promise<T>> defer(Environment env, Dispatcher dispatcher) {
		return new DeferredPromiseSpec<T>().env(env).dispatcher(dispatcher).get();
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return A {@link DeferredPromiseSpec}.
	 */
	public static <T> DeferredPromiseSpec<T> defer() {
		return new DeferredPromiseSpec<T>();
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier
	 * 		{@link Supplier} that will produce the value
	 * @param <T>
	 * 		type of the expected value
	 *
	 * @return A {@link PromiseSpec}.
	 */
	public static <T> PromiseSpec<T> task(Supplier<T> supplier) {
		return new PromiseSpec<T>().supply(supplier);
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and use the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value
	 * 		the value to complete the {@link Promise} with
	 * @param <T>
	 * 		the type of the value
	 *
	 * @return A {@link PromiseSpec} that will produce a {@link Promise} that is completed with the given value
	 */
	public static <T> PromiseSpec<T> success(T value) {
		return new PromiseSpec<T>().success(value);
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error
	 * 		the error to complete the {@link Promise} with
	 * @param <T>
	 * 		the type of the value
	 *
	 * @return A {@link PromiseSpec} that will produce a {@link Promise} that is completed with the given error
	 */
	public static <T> PromiseSpec<T> error(Throwable error) {
		return new PromiseSpec<T>().error(error);
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param promises
	 * 		The promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(Promise<T>... promises) {
		return when(Arrays.asList(promises));
	}

	/**
	 * Merge given deferred promises into a new a {@literal Promise} that will be fulfilled when all of the given
	 * {@literal
	 * Deferred Deferreds} have been fulfilled.
	 *
	 * @param promises
	 * 		The promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(Deferred<T, Promise<T>>... promises) {
		return when(deferredToPromises(promises));
	}

	/**
	 * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise Promises} have been fulfilled.
	 *
	 * @param promises
	 * 		The promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link DeferredPromiseSpec}.
	 */
	public static <T> Promise<List<T>> when(Collection<? extends Promise<T>> promises) {
		Stream<T> deferredStream = new DeferredStreamSpec<T>()
				.synchronousDispatcher()
				.batchSize(promises.size())
				.get()
				.compose();

		Stream<List<T>> aggregatedStream = deferredStream.collect();

		Promise<List<T>> resultPromise = new DeferredPromiseSpec<List<T>>()
				.link(aggregatedStream)
				.get()
				.compose();

		aggregatedStream.connectValues(resultPromise);

		for(Promise<T> promise : promises) {
			promise.connect(deferredStream);
		}

		return resultPromise;
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises
	 * 		The deferred promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<T> any(Deferred<T, Promise<T>>... promises) {
		return any(deferredToPromises(promises));
	}

	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises
	 * 		The deferred promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<T> any(Promise<T>... promises) {
		return any(Arrays.asList(promises));
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises
	 * 		The promises to use.
	 * @param <T>
	 * 		The type of the function result.
	 *
	 * @return a {@link DeferredStreamSpec}.
	 */
	public static <T> Promise<T> any(Collection<? extends Promise<T>> promises) {
		Stream<T> deferredStream = new DeferredStreamSpec<T>()
				.synchronousDispatcher()
				.batchSize(promises.size())
				.get()
				.compose();

		Stream<T> firstStream = deferredStream.first();

		Promise<T> resultPromise = new DeferredPromiseSpec<T>()
				.link(firstStream)
				.get()
				.compose();

		firstStream.connect(resultPromise);

		for(Promise<T> promise : promises) {
			promise.connect(deferredStream);
		}

		return resultPromise;
	}

	private static <T> List<Promise<T>> deferredToPromises(Deferred<T, Promise<T>>... promises) {
		List<Promise<T>> promiseList = new ArrayList<Promise<T>>();
		for(Deferred<T, Promise<T>> deferred : promises) {
			promiseList.add(deferred.compose());
		}
		return promiseList;
	}

}
