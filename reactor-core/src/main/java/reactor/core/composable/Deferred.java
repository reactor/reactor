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

package reactor.core.composable;

import reactor.event.Event;
import reactor.event.support.CallbackEvent;
import reactor.function.Consumer;

/**
 * A Deferred is used to provide a separate between supplying values and consuming values.
 * Values and errors are supplied by calling {@link #accept(Object)} and {@link
 * #accept(Throwable)} respectively. Values can be consumed using the read-only
 * {@link Composable} subclass made available by {@link #compose()}.
 * </p>
 * Typical usage is to create a Deferred and store it internally, only providing the
 * enclosed {@link Composable} to clients. This ensures that clients can only consume values
 * and cannot break the contract by also supplying them.
 *
 * @param <T> The type of the values
 * @param <C> The composable subclass through which the values can be consumed
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Deferred<T, C extends Composable<T>> implements Consumer<T> {

	private final C composable;

	/**
	 * Creates a new Deferred using the given {@link Composable}
	 *
	 * @param composable The composable that will provide access to values
	 */
	public Deferred(C composable) {
		this.composable = composable;
	}

	/**
	 * Accepts the given {@code error} such that it can be consumed by the
	 * underlying {@code Composable}.
	 *
	 * @param error The error to accept
	 */
	public void accept(Throwable error) {
		composable.notifyError(error);
	}

	/**
	 * Accepts the given {@code value} such that is can be consumed by the underlying
	 * {@code Composable}.
	 *
	 * @param value The value to accept
	 */
	@Override
	public void accept(T value) {
		composable.notifyValue(Event.wrap(value));
	}
	/**
	 * Accepts the given {@code value} such that is can be consumed by the underlying
	 * {@code Composable}.
	 *
	 * @param value The value to accept
	 */
	public void acceptEvent(Event<T> value) {
		composable.notifyValue(value);
	}

	/**
	 * Accepts the given {@code value} such that is can be consumed by the underlying
	 * {@code Composable}.
	 *
	 * @param value The value to accept
	 * @param callback the callback
	 */
	public void accept(T value, Consumer<Object> callback) {
		composable.notifyValue(new CallbackEvent<T>(value, callback));
	}

	/**
	 * Returns the underlying {@link Composable} subclass from which values and errors can be
	 * consumed.
	 *
	 * @return The underlying composable
	 */
	public C compose() {
		return composable;
	}

}
