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
import reactor.core.composable.Composable;
import reactor.core.composable.Deferred;
import reactor.core.composable.Stream;
import reactor.core.spec.support.DispatcherComponentSpec;
import reactor.event.dispatch.Dispatcher;

/**
 * @author Jon Brisbin
 */
public final class DeferredStreamSpec<T> extends DispatcherComponentSpec<DeferredStreamSpec<T>, Deferred<T, Stream<T>>> {

	private Composable<?> parent;
	private int batchSize = -1;
	private Iterable<T> values;

	public DeferredStreamSpec<T> link(Composable<?> parent) {
		this.parent = parent;
		return this;
	}

	public DeferredStreamSpec<T> batchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public DeferredStreamSpec<T> each(Iterable<T> values) {
		this.values = values;
		return this;
	}

	@Override
	protected Deferred<T, Stream<T>> configure(Dispatcher dispatcher, Environment env) {
		return new Deferred<T, Stream<T>>(new Stream<T>(env, dispatcher, batchSize, values, parent));
	}

}
