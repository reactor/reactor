/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.fn;

import reactor.fn.registry.Registration;
import reactor.fn.selector.Selector;

/**
 * Basic unit of event handling in Reactor.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public interface Observable {

	/**
	 * Are there any {@link Registration}s with {@link Selector Selectors} that match the given {@code key}.
	 *
   * @param key The key to be matched by {@link Selector Selectors}
   *
	 * @return {@literal true} if there are any matching {@literal Registration}s, {@literal false} otherwise
	 */
	boolean respondsToKey(Object key);

	/**
	 * Register a {@link Consumer} to be triggered when a notification matches the given {@link Selector}.
	 *
	 * @param sel      The {@literal Selector} to be used for matching
	 * @param consumer The {@literal Consumer} to be triggered
	 * @param <E>      The type of the {@link Event}
	 *
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping
	 */
	<E extends Event<?>> Registration<Consumer<E>> on(Selector sel, Consumer<E> consumer);

	/**
	 * Register an {@link Consumer} to be triggered using the internal key that is unique to each
	 * {@literal Observable} instance.
	 *
	 * @param consumer The {@literal Consumer} to be triggered
	 * @param <E>      The type of the {@link Event}
	 *
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping
	 *
	 * @see #notify(Event)
	 * @see #notify(Supplier)
	 */
	<E extends Event<?>> Registration<Consumer<E>> on(Consumer<E> consumer);

	/**
	 * Assign a {@link Function} to receive an {@link Event} and produce a reply of the given type.
	 *
	 * @param sel The {@link Selector} to be used for matching
	 * @param fn  The transformative {@link Function} to call to receive an {@link Event}
	 * @param <E> The type of the {@link Event}
	 * @param <V> The type of the response data
	 *
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping
	 */
	<E extends Event<?>, V> Registration<Consumer<E>> receive(Selector sel, Function<E, V> fn);

	/**
	 * Notify this component that an {@link Event} is ready to be processed and {@link Consumer#accept
	 * accept} {@code onComplete} after dispatching.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @param ev         The {@literal Event}
	 * @param onComplete The callback {@link Consumer}
	 * @param <E>        The type of the {@link Event}
	 *
	 * @return {@literal this}
	 */
	<E extends Event<?>> Observable notify(Object key, E ev, Consumer<E> onComplete);

	/**
	 * Notify this component that an {@link Event} is ready to be processed.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @param ev  The {@literal Event}
   * @param <E> The type of the {@link Event}
	 *
	 * @return {@literal this}
	 */
	<E extends Event<?>> Observable notify(Object key, E ev);

	/**
	 * Notify this component that the given {@link Supplier} can provide an event that's ready to be processed.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event}
	 *
	 * @param <S>      The type of the {@link Supplier}
	 *
	 * @return {@literal this}
	 */
	<S extends Supplier<Event<?>>> Observable notify(Object key, S supplier);

	/**
	 * Notify this component that an {@link Event} is ready to be processed using the internal key that is unique
	 * to each {@literal Observable} instance.
	 *
	 * @param ev  The {@literal Event}
	 *
	 * @return {@literal this}
	 *
	 * @see #on(Consumer)
	 */
	<E extends Event<?>> Observable notify(E ev);

	/**
	 * Notify this component that an {@link Event} is ready to be processed using the internal key that is unique
	 * to each {@link Observable} instance and that the given {@link Supplier} will provide the actual {@link
	 * Event} to publish.
	 *
	 * @param supplier The {@link Supplier} to provide the actual {@link Event}
	 * @param <S>      The type of the {@link Supplier}
	 *
	 * @return {@literal this}
	 *
	 * @see #on(Consumer)
	 */
	<S extends Supplier<Event<?>>> Observable notify(S supplier);

	/**
	 * Notify this component of the given {@link Event} and register an internal {@link Consumer} that will take the output
	 * of a previously-registered {@link Function} and respond using the key set on the {@link Event}'s {@literal replyTo}
	 * property.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @param ev  The {@literal Event}
	 * @param <E> The type of the {@link Event}
	 *
	 * @return {@literal this}
	 */
	<E extends Event<?>> Observable send(Object key, E ev);

	/**
	 * Notify this component that the given {@link Supplier} will provide an {@link Event} and register an internal {@link
	 * Consumer} that will take the output of a previously-registered {@link Function} and respond using the key set on
	 * the {@link Event}'s {@literal replyTo} property.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance
	 *
	 * @return {@literal this}
	 */
	<S extends Supplier<Event<?>>> Observable send(Object key, S supplier);

	/**
	 * Notify this component of the given {@link Event} and register an internal {@link Consumer} that will take the output
	 * of a previously-registered {@link Function} and respond to the key set on the {@link Event}'s {@literal replyTo}
	 * property and will call the {@code notify} method on the given {@link Observable}.
	 *
	 * @param key     The key to be matched by {@link Selector Selectors}
	 * @param ev      The {@literal Event}
	 * @param replyTo The {@link Observable} on which to invoke the notify method
	 * @param <E>     The type of the {@link Event}
	 *
	 * @return {@literal this}
	 */
	<E extends Event<?>> Observable send(Object key, E ev, Observable replyTo);

	/**
	 * Notify this component that the given {@link Supplier} will provide an {@link Event} and register an internal {@link
	 * Consumer} that will take the output of a previously-registered {@link Function} and respond to the key set on the
	 * {@link Event}'s {@literal replyTo} property and will call the {@code notify} method on the given {@link Observable}.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance
	 * @param replyTo  The {@link Observable} on which to invoke the notify method
	 * @param <E>      The type of the {@link Event}
	 *
	 * @return {@literal this}
	 */
	<S extends Supplier<Event<?>>> Observable send(Object key, S supplier, Observable replyTo);

	/**
	 * Notify this component that the consumers registered with a {@link Selector} that matches the {@code key} should be
	 * triggered with a {@literal null} input argument.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 *
	 * @return {@literal this}
	 */
	Observable notify(Object key);

}
