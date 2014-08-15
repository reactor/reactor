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

import org.reactivestreams.Publisher;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.core.Reactor;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selector;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.action.ForEachAction;
import reactor.rx.action.ParallelAction;
import reactor.rx.action.SupplierAction;
import reactor.util.Assert;

import java.util.Arrays;
import java.util.Collection;

/**
 * A public factory to build {@link Stream}.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public abstract class Streams {

	/**
	 * Build a deferred {@literal Stream}, ready to broadcast values.
	 *
	 * @param env the Reactor {@link reactor.core.Environment} to use
	 * @param <T> the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Environment env) {
		return defer(env, env.getDefaultDispatcher());
	}

	/**
	 * Build a deferred synchronous {@literal Stream}, ready to broadcast values.
	 *
	 * @param <T> the type of values passing through the {@literal Stream}
	 * @return a new {@link Stream}
	 */
	public static <T> Stream<T> defer() {
		return new Stream<T>();
	}

	/**
	 * Build a deferred {@literal Stream}, ready to broadcast values.
	 *
	 * @param env        the Reactor {@link reactor.core.Environment} to use
	 * @param dispatcher the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>        the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Environment env, Dispatcher dispatcher) {
		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. " +
				" Refer to #parallel() method. ");
		return new Stream<T>(dispatcher, env, dispatcher.backlogSize() > 0 ?
				(Action.RESERVED_SLOTS > dispatcher.backlogSize() ?
						dispatcher.backlogSize() :
						dispatcher.backlogSize() - Action.RESERVED_SLOTS) :
				Integer.MAX_VALUE);
	}


	/**
	 * Build a deferred concurrent {@link ParallelAction}, ready to broadcast values to the generated sub-streams.
	 * This is a MP-MC scenario type where the parallel action dispatches within the calling dispatcher scope. There
	 * is no
	 * intermediate boundary such as with standard stream like str.buffer().parallel(16) where "buffer" action is run
	 * into a dedicated dispatcher.
	 * <p>
	 * A Parallel action will starve its next available sub-stream to capacity before selecting the next one.
	 * <p>
	 * Will default to {@link Environment#PROCESSORS} number of partitions.
	 * Will default to a new {@link reactor.core.Environment#newDispatcherFactory(int)}} supplier.
	 *
	 * @param <T> the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream} of  {@link reactor.rx.Stream}
	 */
	public static <T> ParallelAction<T> parallel() {
		return parallel(Environment.PROCESSORS);
	}

	/**
	 * Build a deferred concurrent {@link ParallelAction}, ready to broadcast values to the generated sub-streams.
	 * This is a MP-MC scenario type where the parallel action dispatches within the calling dispatcher scope. There
	 * is no
	 * intermediate boundary such as with standard stream like str.buffer().parallel(16) where "buffer" action is run
	 * into a dedicated dispatcher.
	 * <p>
	 * A Parallel action will starve its next available sub-stream to capacity before selecting the next one.
	 * <p>
	 * Will default to {@link Environment#PROCESSORS} number of partitions.
	 * Will default to a new {@link reactor.core.Environment#newDispatcherFactory(int)}} supplier.
	 *
	 * @param <T>      the type of values passing through the {@literal Stream}
	 * @param poolSize the number of maximum parallel sub-streams consuming the broadcasted values.
	 * @return a new {@link reactor.rx.Stream} of  {@link reactor.rx.Stream}
	 */
	public static <T> ParallelAction<T> parallel(int poolSize) {
		return parallel(poolSize, null, Environment.newDispatcherFactory(poolSize));
	}

	/**
	 * Build a deferred concurrent {@link ParallelAction}, ready to broadcast values to the generated sub-streams.
	 * This is a MP-MC scenario type where the parallel action dispatches within the calling dispatcher scope. There
	 * is no
	 * intermediate boundary such as with standard stream like str.buffer().parallel(16) where "buffer" action is run
	 * into a dedicated dispatcher.
	 * <p>
	 * A Parallel action will starve its next available sub-stream to capacity before selecting the next one.
	 * <p>
	 * Will default to {@link reactor.core.Environment#getDefaultDispatcherFactory()} supplier.
	 * Will default to {@link Environment#PROCESSORS} number of partitions.
	 *
	 * @param env the Reactor {@link reactor.core.Environment} to use
	 * @param <T> the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream} of  {@link reactor.rx.Stream}
	 */
	public static <T> ParallelAction<T> parallel(Environment env) {
		return parallel(Environment.PROCESSORS, env, env.getDefaultDispatcherFactory());
	}

	/**
	 * Build a deferred concurrent {@link ParallelAction}, ready to broadcast values to the generated sub-streams.
	 * This is a MP-MC scenario type where the parallel action dispatches within the calling dispatcher scope. There
	 * is no
	 * intermediate boundary such as with standard stream like str.buffer().parallel(16) where "buffer" action is run
	 * into a dedicated dispatcher.
	 * <p>
	 * A Parallel action will starve its next available sub-stream to capacity before selecting the next one.
	 * <p>
	 * Will default to {@link reactor.core.Environment#getDefaultDispatcherFactory()} supplier.
	 *
	 * @param env      the Reactor {@link reactor.core.Environment} to use
	 * @param poolSize the number of maximum parallel sub-streams consuming the broadcasted values.
	 * @param <T>      the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream} of  {@link reactor.rx.Stream}
	 */
	public static <T> ParallelAction<T> parallel(int poolSize, Environment env) {
		return parallel(poolSize, env, env.getDefaultDispatcherFactory());
	}

	/**
	 * Build a deferred concurrent {@link ParallelAction}, accepting data signals to broadcast to a selected generated sub-streams.
	 * This is a MP-MC scenario type where the parallel action dispatches within the calling dispatcher scope. There
	 * is no
	 * intermediate boundary such as with standard stream like str.buffer().parallel(16) where "buffer" action is run
	 * into a dedicated dispatcher.
	 * <p>
	 * A Parallel action will starve its next available sub-stream to capacity before selecting the next one.
	 * <p>
	 * Will default to {@link Environment#PROCESSORS} number of partitions.
	 *
	 * @param env         the Reactor {@link reactor.core.Environment} to use
	 * @param dispatchers the {@link reactor.event.dispatch.Dispatcher} factory to assign each sub-stream with a call to
	 *                    {@link reactor.function.Supplier#get()}
	 * @param <T>         the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream} of  {@link reactor.rx.Stream}
	 */
	public static <T> ParallelAction<T> parallel(Environment env, Supplier<Dispatcher> dispatchers) {
		return parallel(Environment.PROCESSORS, env, dispatchers);
	}

	/**
	 * Build a deferred concurrent {@link ParallelAction}, ready to broadcast values to the generated sub-streams.
	 * This is a MP-MC scenario type where the parallel action dispatches within the calling dispatcher scope. There
	 * is no
	 * intermediate boundary such as with standard stream like str.buffer().parallel(16) where "buffer" action is run
	 * into a dedicated dispatcher.
	 * A Parallel action will starve its next available sub-stream to capacity before selecting the next one.
	 *
	 * @param poolSize    the number of maximum parallel sub-streams consuming the broadcasted values.
	 * @param env         the Reactor {@link reactor.core.Environment} to use
	 * @param dispatchers the {@link reactor.event.dispatch.Dispatcher} factory to assign each sub-stream with a call to
	 *                    {@link reactor.function.Supplier#get()}
	 * @param <T>         the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream} of  {@link reactor.rx.Stream}
	 */
	public static <T> ParallelAction<T> parallel(int poolSize, Environment env, Supplier<Dispatcher> dispatchers) {
		ParallelAction<T> parallelAction = new ParallelAction<T>(SynchronousDispatcher.INSTANCE, dispatchers, poolSize){
			@Override
			protected void onRequest(int n) {
				if(subscription != null){
					subscription.request(n);
				}
			}
		};
		parallelAction.env(env);
		return parallelAction;
	}

	/**
	 * Build a synchronous {@literal Stream}, ready to broadcast values from the given publisher. A publisher will start
	 * producing next elements until onComplete is called.
	 *
	 * @param publisher the publisher to broadcast the Stream subscriber
	 * @param <T>       the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Publisher<T> publisher) {
		return defer(null, SynchronousDispatcher.INSTANCE, publisher);
	}

	/**
	 * Build a deferred {@literal Stream}, ready to broadcast values from the given publisher. A publisher will start
	 * producing next elements until onComplete is called.
	 *
	 * @param publisher the publisher to broadcast the Stream subscriber
	 * @param env       The assigned environment
	 * @param <T>       the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Environment env, Publisher<T> publisher) {
		return defer(env, env.getDefaultDispatcher(), publisher);
	}

	/**
	 * Build a deferred {@literal Stream}, ready to broadcast values from the given publisher. A publisher will start
	 * producing next elements until onComplete is called.
	 *
	 * @param publisher  the publisher to broadcast the Stream subscriber
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to to assign to downstream subscribers
	 * @param <T>        the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Environment env, Dispatcher dispatcher, Publisher<T> publisher) {
		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. To use " +
				"MultiThreadDispatcher, refer to #parallel() method. ");
		Stream<T> stream = defer(env, dispatcher);
		publisher.subscribe(new StreamSpec.StreamSubscriber<T>(stream));
		return stream;
	}

	/**
	 * Return a Specification component to tune the stream properties.
	 *
	 * @param <T> the type of values passing through the {@literal Stream}
	 * @return a new {@link PipelineSpec}
	 */
	public static <T> StreamSpec<T> config() {
		return new StreamSpec<T>();
	}


	/**
	 * Attach a synchronous Stream to the {@link Observable} with the specified {@link Selector}.
	 *
	 * @param observable        the {@link Observable} to observe
	 * @param broadcastSelector the {@link Selector}/{@literal Object} tuple to listen to
	 * @param <T>               the type of values passing through the {@literal Stream}
	 * @return a new {@link Stream}
	 * @since 1.1
	 */
	public static <T> Stream<T> on(Observable observable, Selector broadcastSelector) {
		Dispatcher dispatcher = Reactor.class.isAssignableFrom(observable.getClass()) ?
				((Reactor) observable).getDispatcher() :
				SynchronousDispatcher.INSTANCE;

		Stream<T> stream = defer(null, dispatcher);
		StreamSpec.<T>publisherFrom(observable, broadcastSelector).subscribe(new StreamSpec.StreamSubscriber<T>(stream));
		return stream;
	}

	/**
	 * Build a synchronous {@literal Stream} whose data is generated by the passed supplier on subscription request.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code broadcast()}
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 1.1
	 */
	public static <T> SupplierAction<Void, T> generate(Supplier<T> value) {
		return generate(null, SynchronousDispatcher.INSTANCE, value);
	}

	/**
	 * Build a {@literal Stream} whose data is generated by the passed supplier on subscription request.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code broadcast()}
	 * @param env   The assigned environment
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 1.1
	 */
	public static <T> SupplierAction<Void, T> generate(Environment env, Supplier<T> value) {
		return generate(env, env.getDefaultDispatcher(), value);
	}


	/**
	 * Build a {@literal Stream} whose data is generated by the passed supplier on subscription request.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value      The value to {@code broadcast()}
	 * @param dispatcher The dispatcher to schedule the flush
	 * @param env        The assigned environment
	 * @param <T>        type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 1.1
	 */
	public static <T> SupplierAction<Void, T> generate(Environment env, Dispatcher dispatcher, Supplier<T> value) {
		if (value == null) throw new IllegalArgumentException("Supplier must be provided");
		SupplierAction<Void, T> action = new SupplierAction<Void, T>(dispatcher, value);
		action.capacity(1).env(env);
		return action;
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values The values to {@code broadcastNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	@SafeVarargs
	public static <T> ForEachAction<T> defer(T... values) {
		return defer(Arrays.asList(values));
	}

	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values The values to {@code broadcastNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> ForEachAction<T> defer(Iterable<T> values) {
		return defer(null, SynchronousDispatcher.INSTANCE, values);
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription request.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values The values to {@code broadcast()}
	 * @param env    The assigned environment
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	@SafeVarargs
	public static <T> ForEachAction<T> defer(Environment env, T... values) {
		return defer(env, Arrays.asList(values));
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription request.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values The values to {@code broadcast()}
	 * @param env    The assigned environment
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> ForEachAction<T> defer(Environment env, Iterable<T> values) {
		return defer(env, env.getDefaultDispatcher(), values);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription request.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values     The values to {@code broadcast()}
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the flush
	 * @param <T>        type of the values
	 * @return a {@link Stream} based on the given values
	 */
	@SafeVarargs
	public static <T> ForEachAction<T> defer(Environment env, Dispatcher dispatcher, T... values) {
		return defer(env, dispatcher, Arrays.asList(values));
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription request.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values     The values to {@code broadcast()}
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the flush
	 * @param <T>        type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> ForEachAction<T> defer(Environment env, Dispatcher dispatcher, Iterable<T> values) {
		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. To use " +
				"MultiThreadDispatcher, refer to #parallel() method. ");
		ForEachAction<T> forEachAction = new ForEachAction<T>(values, dispatcher);
		forEachAction.env(env);
		return forEachAction;
	}
}
