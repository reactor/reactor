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
import reactor.core.composable.Composable;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.event.selector.Selector;
import reactor.tuple.Tuple2;

/**
 * A helper class for specifying a {@link Deferred} {@link Promise}.
 *
 * @param <T> The type of the value that the promise will accept
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class DeferredPromiseSpec<T> extends ComposableSpec<DeferredPromiseSpec<T>, Deferred<T, Promise<T>>> {

	private Composable<?> parent;

	/**
	 * Configures the promise to have the given {@code parent}
	 *
	 * @param parent The parent for the promise that's being configured
	 *
	 * @return {@code this}
	 */
	public DeferredPromiseSpec<T> link(Composable<?> parent) {
		this.parent = parent;
		return this;
	}

	@Override
	protected Deferred<T, Promise<T>> createComposable(Environment env, Observable observable,
	                                                   Tuple2<Selector, Object> accept) {
		return new Deferred<T, Promise<T>>(new Promise<T>(observable, env, parent));
	}
}
