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

package reactor.core;

import reactor.fn.Supplier;

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
	 * @param <T> type of the expected value
	 * @return A {@link Deferred.PromiseSpec}.
	 */
	public static <T> Deferred.PromiseSpec<T> defer() {
		return new Deferred.PromiseSpec<T>();
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T>      type of the expected value
	 * @return A {@link Deferred.PromiseSpec}.
	 */
	public static <T> Deferred.PromiseSpec<T> task(Supplier<T> supplier) {
		return new Deferred.PromiseSpec<T>().supplier(supplier);
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Deferred.PromiseSpec} that will produce a {@link Deferred} whose {@link Promise} is completed with
	 *         the given value
	 */
	public static <T> Deferred.PromiseSpec<T> success(T value) {
		return new Deferred.PromiseSpec<T>().value(value);
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and using the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Deferred.PromiseSpec} that will produce a {@link Deferred} whose {@link Promise} is completed with
	 *         the given error
	 */
	public static <T> Deferred.PromiseSpec<T> error(Throwable error) {
		return new Deferred.PromiseSpec<T>().error(error);
	}

	/**
	 * Merge given promises into a new a {@literal Promise}.
	 *
	 * @param promises The promises to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Promise}.
	 */
	public static <T> Promise<List<T>> when(Promise<T>... promises) {
		return when(Arrays.asList(promises));
	}

	/**
	 * Merge given deferred promises into a new a {@literal Promise}.
	 *
	 * @param promises The promises to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Promise}.
	 */
	public static <T> Promise<List<T>> when(Deferred<T, Promise<T>>... promises) {
		return when(deferredToPromises(promises));
	}

	/**
	 * Aggregate given promises into a new a {@literal Promise}.
	 *
	 * @param promises The promises to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Deferred.PromiseSpec}.
	 */
	public static <T> Promise<List<T>> when(Collection<? extends Promise<T>> promises) {
		Stream<T> deferredStream = new Deferred.StreamSpec<T>()
				.sync()
				.batch(promises.size())
				.get()
				.compose();

		Stream<List<T>> aggregatedStream = deferredStream.collect();

		Promise<List<T>> resultPromise = new Deferred.PromiseSpec<List<T>>()
				.sync()
				.link(aggregatedStream)
				.get()
				.compose();

		aggregatedStream.consume(resultPromise);

		for (Promise<T> promise : promises) {
			promise.consume(deferredStream);
		}

		return resultPromise;
	}


	/**
	 * Take the first result coming from the given deferred promises into a new a {@literal Promise}.d
	 *
	 * @param promises The deferred promises to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Promise}.
	 */
	public static <T> Promise<T> any(Deferred<T, Promise<T>>... promises) {
		return any(deferredToPromises(promises));
	}

	/**
	 * Take the first result coming from the given promises into a new a {@literal Promise}.d
	 *
	 * @param promises The deferred promises to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Promise}.
	 */
	public static <T> Promise<T> any(Promise<T>... promises) {
		return any(Arrays.asList(promises));
	}


	/**
	 * Pick the first result coming from the given promises into a new a {@literal Promise}.
	 *
	 * @param promises The promises to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Deferred.PromiseSpec}.
	 */
	public static <T> Promise<T> any(Collection<? extends Promise<T>> promises) {
		Stream<T> deferredStream = new Deferred.StreamSpec<T>()
				.sync()
				.batch(promises.size())
				.get()
				.compose();

		Stream<T> firstStream = deferredStream.first();

		Promise<T> resultPromise = new Deferred.PromiseSpec<T>()
				.sync()
				.link(firstStream)
				.get()
				.compose();

		firstStream.consume(resultPromise);

		for (Promise<T> promise : promises) {
			promise.consume(deferredStream);
		}

		return resultPromise;
	}

	private static <T> List<Promise<T>> deferredToPromises(Deferred<T, Promise<T>>... promises){
		List<Promise<T>> promiseList = new ArrayList<Promise<T>>();
		for(Deferred<T, Promise<T>> deferred : promises){
			promiseList.add(deferred.compose());
		}
		return promiseList;
	}

}
