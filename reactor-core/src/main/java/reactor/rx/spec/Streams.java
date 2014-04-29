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

import org.reactivestreams.api.Producer;
import org.reactivestreams.spi.Publisher;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.core.Reactor;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selector;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.action.ForEachAction;
import reactor.rx.action.SupplierAction;

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
	 * Build a deferred {@literal Stream}, ready to broadcast values.
	 *
	 * @param env        the Reactor {@link reactor.core.Environment} to use
	 * @param dispatcher the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>        the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Environment env, Dispatcher dispatcher) {
		return new Stream<T>(dispatcher, env, Integer.MAX_VALUE);
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
		return defer(publisher, null, SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Build a deferred {@literal Stream}, ready to broadcast values from the given publisher. A publisher will start
	 * producing next elements until onComplete is called.
	 *
	 * @param publisher the publisher to broadcast the Stream subscriber
	 * @param env        The assigned environment
	 * @param <T>       the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Publisher<T> publisher, Environment env) {
		return defer(publisher, env, env.getDefaultDispatcher());
	}

	/**
	 * Build a deferred {@literal Stream}, ready to broadcast values from the given publisher. A publisher will start
	 * producing next elements until onComplete is called.
	 *
	 * @param publisher the publisher to broadcast the Stream subscriber
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to to assign to downstream subscribers
	 * @param <T>       the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Publisher<T> publisher, Environment env, Dispatcher dispatcher) {
		Stream<T> stream = new Stream<T>(dispatcher, Integer.MAX_VALUE).env(env);
		publisher.subscribe(new StreamSpec.StreamSubscriber<T>(stream));
		return stream;
	}


	/**
	 * Build a deferred {@literal Stream}, ready to broadcast values from the given producer. A producer will start
	 * producing next elements until onComplete is called.
	 *
	 * @param producer the publisher to broadcast the Stream subscriber
	 * @param <T>      the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Producer<T> producer) {
		return defer(producer.getPublisher());
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

		final Stream<T> stream = new Stream<T>(dispatcher, Integer.MAX_VALUE);
		StreamSpec.<T>publisherFrom(observable, broadcastSelector).subscribe(new StreamSpec.StreamSubscriber<T>(stream));
		return stream;
	}

	/**
	 * Build a synchronous {@literal Stream} whose data is sourced by the passed element.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code broadcast()}
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the given value
	 */
	public static <T> Stream<T> defer(T value) {
		return defer(value, null, SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Build a {@literal Stream} whose data is sourced by the passed element.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code broadcast()}
	 * @param env        The assigned environment
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the given value
	 */
	public static <T> Stream<T> defer(T value, Environment env) {
		return defer(value, env, env.getDefaultDispatcher());
	}

	/**
	 * Build a {@literal Stream} whose data is sourced by the passed element.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code broadcast()}
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the value
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the given value
	 */
	@SuppressWarnings("unchecked")
	public static <T> ForEachAction<T> defer(T value, Environment env, Dispatcher dispatcher) {
		ForEachAction<T> forEachAction = new ForEachAction<T>(Arrays.asList(value), dispatcher);
		forEachAction.prefetch(1).env(env);
		return forEachAction;
	}

	/**
	 * Build a synchronous {@literal Stream} whose data is generated by the passed supplier on flush event from
	 * {@link reactor.rx.action.Flushable#onFlush()}.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code broadcast()}
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 1.1
	 */
	public static <T> SupplierAction<Void, T> defer(Supplier<T> value) {
		return defer(value, null, SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Build a {@literal Stream} whose data is generated by the passed supplier on flush event from
	 * {@link reactor.rx.action.Flushable#onFlush()}.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code broadcast()}
	 * @param env        The assigned environment
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 1.1
	 */
	public static <T> SupplierAction<Void, T> defer(Supplier<T> value, Environment env) {
		return defer(value, env, env.getDefaultDispatcher());
	}


	/**
	 * Build a {@literal Stream} whose data is generated by the passed supplier on flush event from
	 * {@link reactor.rx.action.Flushable#onFlush()}.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value      The value to {@code broadcast()}
	 * @param dispatcher The dispatcher to schedule the flush
	 * @param env        The assigned environment
	 * @param <T>        type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 1.1
	 */
	public static <T> SupplierAction<Void, T> defer(Supplier<T> value, Environment env, Dispatcher dispatcher) {
		if(value == null) throw new IllegalArgumentException("Supplier must be provided");
		SupplierAction<Void, T> action = new SupplierAction<Void, T>(dispatcher, value);
		action.prefetch(1).env(env);
		return action;
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on flush event
	 * from {@link reactor.rx.action.Flushable#onFlush()}.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values The values to {@code broadcast()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> ForEachAction<T> defer(Iterable<T> values) {
		return defer(values, null, SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on flush event
	 * from {@link reactor.rx.action.Flushable#onFlush()}.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values The values to {@code broadcast()}
	 * @param env        The assigned environment
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> ForEachAction<T> defer(Iterable<T> values, Environment env) {
		return defer(values, env, env.getDefaultDispatcher());
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on flush event
	 * from {@link reactor.rx.action.Flushable#onFlush()}.
	 * If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values The values to {@code broadcast()}
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the flush
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> ForEachAction<T> defer(Iterable<T> values, Environment env, Dispatcher dispatcher) {
		ForEachAction<T> forEachAction = new ForEachAction<T>(values, dispatcher);
		forEachAction.env(env);
		return forEachAction;
	}

}
