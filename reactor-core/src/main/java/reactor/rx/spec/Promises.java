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

package reactor.rx.spec;

import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.function.Supplier;
import reactor.rx.Promise;
import reactor.rx.action.MergeAction;
import reactor.rx.action.SupplierAction;
import reactor.util.Assert;

import java.util.Arrays;
import java.util.List;

/**
 * Helper methods for creating {@link reactor.rx.Promise} instances.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public abstract class Promises {


	/**
	 * Return a Specification component to tune the stream properties.
	 *
	 * @param <T> the type of values passing through the {@literal Stream}
	 * @return a new {@link PipelineSpec}
	 */
	public static <T> PromiseSpec<T> config() {
		return new PromiseSpec<T>();
	}

	/**
	 * Create a {@link Promise}.
	 *
	 * @param env the {@link reactor.core.Environment} to use
	 * @param <T> type of the expected value
	 * @return a new {@link reactor.rx.Promise}
	 */
	public static <T> Promise<T> defer(Environment env) {
		return defer(env, env.getDefaultDispatcher());
	}

	/**
	 * Create a {@link Promise}.
	 *
	 * @param env        the {@link reactor.core.Environment} to use
	 * @param dispatcher the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>        type of the expected value
	 * @return a new {@link reactor.rx.Promise}
	 */
	public static <T> Promise<T> defer(Environment env, Dispatcher dispatcher) {
		return new Promise<T>(dispatcher, env);
	}

	/**
	 * Create a synchronous {@link Promise}.
	 *
	 * @param <T> type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> defer() {
		return defer(null, SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Create a synchronous {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T>      type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> syncTask(Supplier<T> supplier) {
		return task(null, SynchronousDispatcher.INSTANCE, supplier);
	}

	/**
	 * Create a {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param env      The assigned environment
	 * @param <T>      type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> task(Environment env, Supplier<T> supplier) {
		return task(env, env.getDefaultDispatcher(), supplier);
	}

	/**
	 * Create a {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier   {@link Supplier} that will produce the value
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the value
	 * @param <T>        type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> task(Environment env, Dispatcher dispatcher, Supplier<T> supplier) {
		SupplierAction<Void, T> supplierAction = new SupplierAction<Void, T>(dispatcher, supplier);
		Promise<T> promise = new Promise<T>(dispatcher, env);
		supplierAction.subscribe(promise);
		return promise;
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(T value) {
		return success(null, SynchronousDispatcher.INSTANCE, value);
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param env   The assigned environment
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(Environment env, T value) {
		return success(env, env.getDefaultDispatcher(), value);
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value      the value to complete the {@link Promise} with
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the value
	 * @param <T>        the type of the value
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(Environment env, Dispatcher dispatcher, T value) {
		return new Promise<T>(value, dispatcher, env);
	}

	/**
	 * Create synchronous {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Throwable error) {
		return error(null, SynchronousDispatcher.INSTANCE, error);
	}

	/**
	 * Create a {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param env   The assigned environment
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Environment env, Throwable error) {
		return error(env, env.getDefaultDispatcher(), error);
	}

	/**
	 * Create a {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error      the error to complete the {@link Promise} with
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the value
	 * @param <T>        the type of the value
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Environment env, Dispatcher dispatcher, Throwable error) {
		return new Promise<T>(error, dispatcher, env);
	}

	/**
	 * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise Promises} have been fulfilled.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(Promise<T>... promises) {
		return when(Arrays.asList(promises));
	}

	/**
	 * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise Promises} have been fulfilled.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(final List<? extends Promise<T>> promises) {
		Assert.isTrue(promises.size() > 0, "Must aggregate at least one promise");

		return new MergeAction<T>(SynchronousDispatcher.INSTANCE, null, promises)
				.env(promises.get(0).getEnvironment())
				.buffer(promises.size())
				.next();
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises The deferred promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<T> any(Promise<T>... promises) {
		return any(Arrays.asList(promises));
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(List<? extends Promise<T>> promises) {
		Assert.isTrue(promises.size() > 0, "Must aggregate at least one promise");

		MergeAction<T> mergeAction = new MergeAction<T>(SynchronousDispatcher.INSTANCE, null, promises);
		mergeAction.env(promises.get(0).getEnvironment());

		return mergeAction.next();
	}

}
