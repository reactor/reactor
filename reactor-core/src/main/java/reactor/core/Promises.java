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

/**
 * Helper methods for creating {@link Deferred} instances, backed by a {@link Promise}.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public abstract class Promises {

	/**
	 * Create a {@link Deferred} backed by a {@link Promise}.
	 *
	 * @param <T> type of the expected value
	 * @return A {@link Deferred.PromiseSpec}.
	 */
	public static <T> Deferred.PromiseSpec<T> defer() {
		return new Deferred.PromiseSpec<T>();
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T>      type of the expected value
	 * @return A {@link Deferred.PromiseSpec}.
	 */
	public static <T> Deferred.PromiseSpec<T> task(Supplier<T> supplier) {
		return new Deferred.PromiseSpec<T>().supplier(supplier);
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Deferred.PromiseSpec} that will produce a {@link Deferred} whose {@link Promise} is completed with
	 *         the given value
	 */
	public static <T> Deferred.PromiseSpec<T> success(T value) {
		return new Deferred.PromiseSpec<T>().value(value);
	}

	/**
	 * Create a {@link Deferred} backed by a {@link Promise} and using the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Deferred.PromiseSpec} that will produce a {@link Deferred} whose {@link Promise} is completed with
	 *         the given error
	 */
	public static <T> Deferred.PromiseSpec<T> error(Throwable error) {
		return new Deferred.PromiseSpec<T>().error(error);
	}

}
