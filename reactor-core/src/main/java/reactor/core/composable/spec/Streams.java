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
import reactor.core.Observable;
import reactor.core.composable.Deferred;
import reactor.core.composable.Stream;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selector;
import reactor.function.Supplier;
import reactor.tuple.Tuple2;

import java.util.Arrays;
import java.util.Collection;

/**
 * A public factory to build {@link Stream Streams} that use a {@link SynchronousDispatcher}.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public abstract class Streams {

	/**
	 * Build a deferred {@literal Stream}, ready to accept values.
	 *
	 * @param env
	 * 		the Reactor {@link reactor.core.Environment} to use
	 * @param <T>
	 * 		the type of values passing through the {@literal Stream}
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Stream<T>> defer(Environment env) {
		return defer(env, env.getDefaultDispatcher());
	}

	/**
	 * Build a deferred {@literal Stream}, ready to accept values.
	 *
	 * @param env
	 * 		the Reactor {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the name of the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>
	 * 		the type of values passing through the {@literal Stream}
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Stream<T>> defer(Environment env, String dispatcher) {
		return defer(env, env.getDispatcher(dispatcher));
	}

	/**
	 * Build a deferred {@literal Stream}, ready to accept values.
	 *
	 * @param env
	 * 		the Reactor {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>
	 * 		the type of values passing through the {@literal Stream}
	 *
	 * @return a new {@link reactor.core.composable.Deferred}
	 */
	public static <T> Deferred<T, Stream<T>> defer(Environment env, Dispatcher dispatcher) {
		return new DeferredStreamSpec<T>().env(env).dispatcher(dispatcher).get();
	}

	/**
	 * Build a deferred {@literal Stream}, ready to accept values.
	 *
	 * @param <T>
	 * 		the type of values passing through the {@literal Stream}
	 *
	 * @return a new {@link DeferredStreamSpec}
	 */
	public static <T> DeferredStreamSpec<T> defer() {
		return new DeferredStreamSpec<T>();
	}


	/**
	 * Attach a Stream to the {@link Observable} with the specified {@link Selector} and key.
	 *
	 * @param observable
	 * 		the {@link Observable} to observe
	 * @param acceptSelector
	 * 		the {@link Selector}/{@literal Object} tuple to listen to
	 * @param key
	 * 		the key to publish to
	 * @param <T>
	 * 		the type of values passing through the {@literal Stream}
	 *
	 * @return a new {@link DeferredStreamSpec}
	 * @since 1.1
	 */
	public static <T> Stream<T> on(Observable observable, Selector acceptSelector, Object key) {
		return new StreamSpec<T>().observable(observable).acceptSelector(Tuple2.of(acceptSelector,key)).get();
	}

	/**
	 * Attach a Stream to the {@link Observable} with the specified {@link Selector}.
	 *
	 * @param observable
	 * 		the {@link Observable} to observe
	 * @param acceptSelector
	 * 		the {@link Selector}/{@literal Object} tuple to listen/publish to
	 * @param <T>
	 * 		the type of values passing through the {@literal Stream}
	 *
	 * @return a new {@link DeferredStreamSpec}
	 * @since 1.1
	 */
	public static <T> Stream<T> on(Observable observable, Selector acceptSelector) {
		return on(observable, acceptSelector, acceptSelector.getObject());
	}

	/**
	 * Build a deferred {@literal Stream} that will implicitly {@link Deferred#accept(Object)}
	 * the given value whenever the {@link reactor.core.composable.Stream#flush()} function
	 * is invoked.
	 *
	 * @param value
	 * 		The value to {@code accept()}
	 * @param <T>
	 * 		type of the value
	 *
	 * @return a {@link DeferredStreamSpec} based on the given value
	 */
	@SuppressWarnings("unchecked")
	public static <T> StreamSpec<T> defer(T value) {
		return  new StreamSpec<T>().each(Arrays.asList(value)).batchSize(1);
	}

	/**
	 * Build a deferred {@literal Stream} that will implicitly {@link Deferred#accept(Object)}
	 * the supplied value whenever the {@link reactor.core.composable.Stream#flush()} function
	 * is invoked.
	 *
	 * @param value
	 * 		The value to {@code accept()}
	 * @param <T>
	 * 		type of the value
	 *
	 * @return a {@link DeferredStreamSpec} based on the given value
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public static <T> StreamSpec<T> defer(Supplier<T> value) {
		return  new StreamSpec<T>().generate(value).batchSize(1);
	}

	/**
	 * Build a deferred {@literal Stream} that will implicitly {@link Deferred#accept(Object)}
	 * the given values whenever the {@link reactor.core.composable.Stream#flush()} function
	 * is invoked. If the {@code values} are a {@code Collection} the Stream's batch size will
	 * be set to the Collection's {@link Collection#size()}.
	 *
	 * @param values
	 * 		The values to {@code accept()}
	 * @param <T>
	 * 		type of the values
	 *
	 * @return a {@link StreamSpec} based on the given values
	 */
	public static <T> StreamSpec<T> defer(Iterable<T> values) {
		int batchSize = (values instanceof Collection ? ((Collection<?>)values).size() : -1);
		return new StreamSpec<T>().each(values).batchSize(batchSize);
	}

}
