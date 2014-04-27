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

import org.reactivestreams.spi.Subscription;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.function.Supplier;
import reactor.rx.Promise;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.action.CollectAction;
import reactor.rx.action.MergeAction;
import reactor.rx.action.SupplierAction;
import reactor.util.Assert;

import java.util.Collection;
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
		return new Promise<T>(new Action<T, T>(dispatcher), env);
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
	public static <T> Promise<T> task(Supplier<T> supplier) {
		return task(supplier, null, SynchronousDispatcher.INSTANCE);
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
	public static <T> Promise<T> task(Supplier<T> supplier, Environment env) {
		return task(supplier, env, env.getDefaultDispatcher());
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
	public static <T> Promise<T> task(Supplier<T> supplier, Environment env, Dispatcher dispatcher) {
		SupplierAction<Void, T> supplierAction = new SupplierAction<Void, T>(dispatcher, supplier);
		Promise<T> promise = new Promise<T>(supplierAction, env);
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
		return success(value, null, SynchronousDispatcher.INSTANCE);
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
	public static <T> Promise<T> success(T value, Environment env) {
		return success(value, env, env.getDefaultDispatcher());
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
	public static <T> Promise<T> success(T value, Environment env, Dispatcher dispatcher) {
		return new Promise<T>(value, new Action<T, T>(dispatcher), env);
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
		return error(error, null, SynchronousDispatcher.INSTANCE);
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
	public static <T> Promise<T> error(Throwable error, Environment env) {
		return error(error, env, env.getDefaultDispatcher());
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
	public static <T> Promise<T> error(Throwable error, Environment env, Dispatcher dispatcher) {
		return new Promise<T>(error, new Action<T, T>(dispatcher), env);
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
		Assert.isTrue(promises.length > 0, "Must aggregate at least one promise");

		CollectAction<T> collectAction = new CollectAction<T>(promises.length, SynchronousDispatcher.INSTANCE);
		collectAction.env(promises[0].getEnvironment());
		Promise<List<T>> resultPromise = next(collectAction);

		new MergeAction<T>(SynchronousDispatcher.INSTANCE, promises.length, collectAction, promises);

		return resultPromise;
	}

	/**
	 * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise Promises} have been fulfilled.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(Collection<? extends Promise<T>> promises) {
		return when(toArray(promises));
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises The deferred promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<T> any(Promise<T>... promises) {
		Assert.isTrue(promises.length > 0, "Must aggregate at least one promise");

		MergeAction<T> mergeAction = new MergeAction<T>(SynchronousDispatcher.INSTANCE, promises.length, promises);
		mergeAction.env(promises[0].getEnvironment());

		return Promise.wrap(mergeAction);
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(Collection<? extends Promise<T>> promises) {

		return any(toArray(promises));
	}

	/**
	 * Consume the next value of the given {@link reactor.rx.Stream} and fulfill the returned {@link
	 * reactor.rx.Promise} on the next value.
	 *
	 * @param composable the {@literal Composable} to consume the next value from
	 * @param <T>        type of the value
	 * @return a {@link reactor.rx.Promise} that will be fulfilled with the next value coming into the given
	 * Composable
	 */
	public static <T> Promise<T> next(Stream<T> composable) {
		final Promise<T> resultPromise = new Promise<T>(
				new Action<T, T>(),
				composable.getEnvironment());

		composable.connect(new Action<T, T>(SynchronousDispatcher.INSTANCE, 1) {

			@Override
			protected void doSubscribe(Subscription subscription) {
				subscription.requestMore(1);
			}

			@Override
			protected void doFlush() {
				resultPromise.broadcastFlush();
			}

			@Override
			protected void doComplete() {
				resultPromise.broadcastComplete();
			}

			@Override
			protected void doNext(T ev) {
				resultPromise.broadcastNext(ev);
			}

			@Override
			protected void doError(Throwable ev) {
				resultPromise.broadcastError(ev);
			}
		});

		return resultPromise;
	}

	@SuppressWarnings("unchecked")
	static private <O> Promise<O>[] toArray(Collection<? extends Promise<O>> promises) {
		Promise<O>[] arrayPromises = new Promise[promises.size()];
		int i = 0;
		for (Promise<O> promise : promises) {
			arrayPromises[i++] = promise;
		}
		return arrayPromises;
	}

}
