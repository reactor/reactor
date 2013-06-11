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

import java.util.Arrays;
import java.util.Collection;

/**
 * A public factory to build {@link Promise}
 *
 * @author Stephane Maldini
 */
public abstract class Promises {
	/**
	 * Create an empty {@link reactor.core.Promise}.
	 *
	 * @param <T> The type of the object to be set on the {@link reactor.core.Promise}.
	 * @return The new {@link reactor.core.Promise}.
	 */
	public static <T> Promise.Spec<T> defer() {
		return new Promise.Spec<T>(null, null, null, null);
	}

	/**
	 * Create a new {@link reactor.core.Promise} based on the given {@link Throwable}.
	 *
	 * @param reason The exception to set.
	 * @param <T>    The type of the expected {@link reactor.core.Promise}.
	 * @return The new {@link reactor.core.Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise.Spec<T> error(Throwable reason) {
		return (Promise.Spec<T>) new Promise.Spec<Throwable>(null, null, reason, null);
	}

	/**
	 * Create a {@literal Promise} based on the given value.
	 *
	 * @param value The value to use.
	 * @param <T>   The type of the value.
	 * @return a {@link reactor.core.Promise.Spec}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise.Spec<T> success(T value) {
		return new Promise.Spec<T>(value, null, null, null);
	}

	/**
	 * Create a {@literal Promise} based on the given supplier.
	 *
	 * @param supplier The value to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link reactor.core.Promise.Spec}.
	 */
	public static <T> Promise.Spec<T> task(Supplier<T> supplier) {
		return new Promise.Spec<T>(null, supplier, null, null);
	}

	/**
	 * Merge given composable into a new a {@literal Promise}.
	 *
	 * @param composables The composables to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Promise.Spec}.
	 */
	public static <T> Promise.Spec<Collection<T>> when(Composable<T>... composables) {
		return when(Arrays.asList(composables));
	}

	/**
	 * Merge given composable into a new a {@literal Promise}.
	 *
	 * @param composables The composables to use.
	 * @param <T>         The type of the function result.
	 * @return a {@link reactor.core.Promise.Spec}.
	 */
	public static <T> Promise.Spec<Collection<T>> when(Collection<? extends Composable<T>> composables) {
		return new Promise.Spec<Collection<T>>(null, null, null, composables);
	}
}
