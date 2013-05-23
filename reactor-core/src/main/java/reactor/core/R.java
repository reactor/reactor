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

package reactor.core;

import reactor.fn.Supplier;

import java.util.Arrays;

/**
 * Base class to encapsulate commonly-used functionality around Reactors.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class R {

	/**
	 * Create a {@literal Reactor} based on the given id.
	 *
	 * @return The new {@link Reactor}.
	 */
	public static Reactor.Builder reactor() {
		return new Reactor.Builder(null);
	}

	/**
	 * Create a delayed {@link Composable} with no initial state, ready to accept values.
	 *
	 * @return A {@link Composable.Builder} to further refine the {@link Composable} and then build it.
	 */
	public static <T> Composable.Builder<T> compose() {
		return new Composable.Builder<T>();
	}

	/**
	 * Create a {@link Composable} from the given value.
	 *
	 * @param value The value to use.
	 * @param <T>   The type of the value.
	 * @return A {@link Composable.Builder} to further refine the {@link Composable} and then build it.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Composable.Builder<T> compose(T value) {
		return new Composable.Builder<T>(Arrays.asList(value));
	}

	/**
	 * Create a {@link Composable} from the given list of values.
	 *
	 * @param values The values to use.
	 * @param <T>    The type of the values.
	 * @return A {@link Composable.Builder} to further refine the {@link Composable} and then build it.
	 */
	public static <T> Composable.Builder<T> compose(Iterable<T> values) {
		return new Composable.Builder<T>(values);
	}

	/**
	 * Create a {@link Composable} from the given {@link reactor.fn.Supplier}.
	 *
	 * @param supplier The function to defer.
	 * @param <T>      The type of the values.
	 * @return A {@link Composable.Builder} to further refine the {@link Composable} and then build it.
	 */
	public static <T> Composable.Builder<T> compose(Supplier<T> supplier) {
		return new Composable.Builder<T>(supplier);
	}

	/**
	 * Create a {@literal Promise} based on the given exception.
	 *
	 * @param reason The exception to use as the value.
	 * @param <T>    The type of the intended {@literal Promise} value.
	 * @return a {@link Promise.Builder} to further refine the {@link Promise} and then build it..
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise.Builder<T> promise(Throwable reason) {
		return (Promise.Builder<T>) new Promise.Builder<Throwable>(reason);
	}

	/**
	 * Create a {@literal Promise} based on the given value.
	 *
	 * @param value The value to use.
	 * @param <T>   The type of the value.
	 * @return a {@link Promise.Builder}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise.Builder<T> promise(T value) {
		return new Promise.Builder<T>(value);
	}

	/**
	 * Create a {@literal Promise} based on the given supplier.
	 *
	 * @param supplier The value to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise.Builder}.
	 */
	public static <T> Promise.Builder<T> promise(Supplier<T> supplier) {
		return new Promise.Builder<T>(supplier);
	}
}


