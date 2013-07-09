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

import reactor.core.Reactor;
import reactor.core.composable.Composable;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.spec.support.DispatcherComponentSpec;

/**
 * @author Jon Brisbin
 */
public class DeferredPromiseSpec<T> extends DispatcherComponentSpec<DeferredPromiseSpec<T>, Deferred<T, Promise<T>>> {

	private Composable<?> parent;

	public DeferredPromiseSpec<T> link(Composable<?> parent) {
		this.parent = parent;
		return this;
	}

	@Override
	protected Deferred<T, Promise<T>> configure(Reactor reactor) {
		return new Deferred<T, Promise<T>>(new Promise<T>(env, reactor, parent, null, null, null));
	}
}
