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

import reactor.function.Functions;
import reactor.core.Reactor;
import reactor.core.composable.Composable;
import reactor.core.composable.Promise;
import reactor.core.spec.support.DispatcherComponentSpec;
import reactor.function.Supplier;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class PromiseSpec<T> extends DispatcherComponentSpec<PromiseSpec<T>, Promise<T>> {

	private Composable<?> parent;
	private T             value;
	private Supplier<T>   valueSupplier;
	private Throwable     error;

	public PromiseSpec<T> link(Composable<?> parent) {
		this.parent = parent;
		return this;
	}

	public PromiseSpec<T> success(T value) {
		Assert.isNull(error, "Cannot set both a value and an error. Use one or the other.");
		Assert.isNull(valueSupplier, "Cannot set both a value and a Supplier. Use one or the other.");
		this.value = value;
		return this;
	}

	public PromiseSpec<T> success(Supplier<T> valueSupplier) {
		Assert.isNull(error, "Cannot set both an error and a Supplier. Use one or the other.");
		Assert.isNull(value, "Cannot set both a value and a Supplier. Use one or the other.");
		this.valueSupplier = valueSupplier;
		return this;
	}

	public PromiseSpec<T> error(Throwable error) {
		Assert.isNull(value, "Cannot set both a value and an error. Use one or the other.");
		Assert.isNull(valueSupplier, "Cannot set both an error and a Supplier. Use one or the other.");
		this.error = error;
		return this;
	}

	@Override
	protected Promise<T> configure(Reactor reactor) {
		return new Promise<T>(env, reactor.getDispatcher(), parent, (null != value ? Functions.supplier(value) : null), error, valueSupplier);
	}
}
