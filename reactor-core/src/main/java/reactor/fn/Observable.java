/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package reactor.fn;

/**
 * Basic unit of event handling in Reactor.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface Observable {

	/**
	 * Are there any {@link Registration}s that match the given {@link Selector}.
	 *
	 * @param sel The right-hand side of the {@link Selector} comparison.
	 * @return {@literal true} if there are any {@literal Registration}s assigned, {@literal false} otherwise.
	 */
	boolean respondsTo(Selector sel);

	/**
	 * Register a {@link Consumer} to be triggered using the given {@link Selector}.
	 *
	 * @param sel      The left-hand side of the {@link Selector} comparison.
	 * @param consumer The {@literal Consumer} to be triggered.
	 * @param <T>      The type of the data in the {@link Event}.
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping.
	 */
	<T, E extends Event<T>> Registration<Consumer<E>> on(Selector sel, Consumer<E> consumer);

	/**
	 * Register an {@link Consumer} to be triggered using the internal, global {@link Selector} that is unique to each
	 * {@literal Observable} instance.
	 *
	 * @param consumer The {@literal Consumer} to be triggered.
	 * @param <T>      The type of the data in the {@link Event}.
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping.
	 */
	<T, E extends Event<T>> Registration<Consumer<E>> on(Consumer<E> consumer);

	/**
	 * Assign a {@link Function} to receive an {@link Event} and produce a reply of the given type.
	 *
	 * @param sel The left-hand side of the {@link Selector} comparison.
	 * @param fn  The transformative {@link Function} to call to receive an {@link Event}.
	 * @param <T> The type of the request data.
	 * @param <V> The type of the response data.
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping.
	 */
	<T, E extends Event<T>, V> Registration<Consumer<E>> receive(Selector sel, Function<E, V> fn);

	/**
	 * Notify this component that an {@link Event} is ready to be processed and accept onComplete after dispatching.
	 *
	 * @param sel        The right-hand side of the {@link Selector} comparison.
	 * @param ev         The {@literal Event}.
	 * @param onComplete The callback {@link Consumer}.
	 * @param <T>        The type of the data in the {@link Event}.
	 * @return {@literal this}
	 */
	<T, E extends Event<T>> Observable notify(Selector sel, E ev, Consumer<E> onComplete);

	/**
	 * Notify this component that an {@link Event} is ready to be processed.
	 *
	 * @param sel The right-hand side of the {@link Selector} comparison.
	 * @param ev  The {@literal Event}.
	 * @param <T> The type of the data in the {@link Event}.
	 * @return {@literal this}
	 */
	<T, E extends Event<T>> Observable notify(Selector sel, E ev);

	/**
	 * Notify this component that the given {@link Supplier} can provide an event that's ready to be processed.
	 *
	 * @param sel      The right-hand side of the {@link Selector} comparison.
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event}.
	 * @param <T>      The type of the data in the {@link Event}.
	 * @param <S>      The type of the {@link Supplier}.
	 * @return {@literal this}
	 */
	<T, S extends Supplier<Event<T>>> Observable notify(Selector sel, S supplier);

	/**
	 * Notify this component that an {@link Event} is ready to be processed using the internal, global {@link Selector}
	 * that is unique to each {@literal Observable} instance.
	 *
	 * @param ev  The {@literal Event}.
	 * @param <T> The type of the data in the {@link Event}.
	 * @return {@literal this}
	 */
	<T, E extends Event<T>> Observable notify(E ev);

	/**
	 * Notify this component that an {@link Event} is ready to be processed using the internal, global {@link Selector}
	 * that is unique to each {@link Observable} instance and that the given {@link Supplier} will provide the actual
	 * {@link Event} to publish.
	 *
	 * @param supplier The {@link Supplier} to provide the actual {@link Event}.
	 * @param <T>      The type of the data in the {@link Event}.
	 * @param <S>      The type of the {@link Supplier}.
	 * @return {@literal this}
	 */
	<T, S extends Supplier<Event<T>>> Observable notify(S supplier);

	/**
	 * Notify this component of the given {@link Event} and register an internal {@link Consumer} that will take the output
	 * of a previously-registered {@link Function} and respond to the {@link Selector} set on the {@link Event}'s {@literal
	 * replyTo} property.
	 *
	 * @param sel The right-hand side of the {@link Selector} comparison.
	 * @param ev  The {@literal Event}.
	 * @param <T> The type of the request data.
	 * @return {@literal this}
	 */
	<T, E extends Event<T>> Observable send(Selector sel, E ev);

	/**
	 * Notify this component that the given {@link Supplier} will provide an {@link Event} and register an internal {@link
	 * Consumer} that will take the output of a previously-registered {@link Function} and respond to the {@link Selector}
	 * set on the {@link Event}'s {@literal replyTo} property.
	 *
	 * @param sel      The right-hand side of the {@link Selector} comparison.
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance.
	 * @param <T>      The type of the request data.
	 * @return {@literal this}
	 */
	<T, S extends Supplier<Event<T>>> Observable send(Selector sel, S supplier);

	/**
	 * Notify this component of the given {@link Event} and register an internal {@link Consumer} that will take the output
	 * of a previously-registered {@link Function} and respond to the {@link Selector} set on the {@link Event}'s {@literal
	 * replyTo} property and will call the {@code notify} method on the given {@link Observable}.
	 *
	 * @param sel     The right-hand side of the {@link Selector} comparison.
	 * @param ev      The {@literal Event}.
	 * @param replyTo The {@link Observable} on which to invoke the notify method.
	 * @param <T>     The type of the request data.
	 * @return {@literal this}
	 */
	<T, E extends Event<T>> Observable send(Selector sel, E ev, Observable replyTo);

	/**
	 * Notify this component that the given {@link Supplier} will provide an {@link Event} and register an internal {@link
	 * Consumer} that will take the output of a previously-registered {@link Function} and respond to the {@link Selector}
	 * set on the {@link Event}'s {@literal replyTo} property and will call the {@code notify} method on the given {@link
	 * Observable}.
	 *
	 * @param sel      The right-hand side of the {@link Selector} comparison.
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance.
	 * @param replyTo  The {@link Observable} on which to invoke the notify method.
	 * @param <T>      The type of the request data.
	 * @return {@literal this}
	 */
	<T, S extends Supplier<Event<T>>> Observable send(Selector sel, S supplier, Observable replyTo);

	/**
	 * Notify this component that the consumers assigned to this {@link Selector} should be triggered with a {@literal
	 * null} input argument.
	 *
	 * @param sel The right-hand side of the {@link Selector} comparison.
	 * @return {@literal this}
	 */
	Observable notify(Selector sel);

}
