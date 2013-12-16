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
import reactor.core.composable.Promise;
import reactor.event.selector.Selector;
import reactor.function.Supplier;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

/**
 * A helper class for specifying a {@link Promise}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 *
 * @param <T> the type of the value that the Promise will contain
 */
public final class PromiseSpec<T> extends ComposableSpec<PromiseSpec<T>, Promise<T>> {

	private T             value;
	private Supplier<T>   valueSupplier;
	private Throwable     error;

	/**
	 * Configures the promise to have been successfully completed with the given {@code value}.
	 *
	 * @param value The value for the promise to contain
	 *
	 * @return {@code this}
	 */
	public PromiseSpec<T> success(T value) {
		Assert.isNull(error, "Cannot set both a value and an error. Use one or the other.");
		Assert.isNull(valueSupplier, "Cannot set both a value and a Supplier. Use one or the other.");
		this.value = value;
		return this;
	}

	/**
	 * Configures the promise to have been successfully completed with the value from the given
	 * {@code valueSupplier}.
	 *
	 * @param valueSupplier The supplier of the value for the promise to contain
	 *
	 * @return {@code this}
	 */
	public PromiseSpec<T> supply(Supplier<T> valueSupplier) {
		Assert.isNull(error, "Cannot set both an error and a Supplier. Use one or the other.");
		Assert.isNull(value, "Cannot set both a value and a Supplier. Use one or the other.");
		this.valueSupplier = valueSupplier;
		return this;
	}

	/**
	 * Configures the promise to have been completed with the given {@code error}.
	 *
	 * @param error The error to be held by the Promise
	 *
	 * @return {@code this}
	 */
	public PromiseSpec<T> error(Throwable error) {
		Assert.isNull(value, "Cannot set both a value and an error. Use one or the other.");
		Assert.isNull(valueSupplier, "Cannot set both an error and a Supplier. Use one or the other.");
		this.error = error;
		return this;
	}

	@Override
	protected Promise<T> createComposable(Environment env, Observable observable,
	                                      Tuple2<Selector, Object> accept) {
		if (value != null) {
			return new Promise<T>(value, observable, env);
		} else if (valueSupplier != null) {
			return new Promise<T>(valueSupplier, observable, env);
		} else if (error != null) {
			return new Promise<T>(error, observable, env);
		} else {
			throw new IllegalStateException("A success value/supplier or error reason must be provided. Use " +
				DeferredPromiseSpec.class.getSimpleName() + " to create a deferred promise");
		}
	}
}
