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

import reactor.fn.Consumer;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Deferred<T, C extends Composable<T>> implements Consumer<T> {

	private final C composable;

	public Deferred(C composable) {
		this.composable = composable;
	}

	public void accept(Throwable error) {
		composable.notifyError(error);
	}

	@Override
	public void accept(T value) {
		composable.notifyValue(value);
	}

	public C compose() {
		return composable;
	}

	public static class PromiseSpec<T> extends ComponentSpec<PromiseSpec<T>, Deferred<T, Promise<T>>> {
		private Composable<?> parent;

		public PromiseSpec<T> link(Composable<?> parent) {
			this.parent = parent;
			return this;
		}

		@Override
		protected Deferred<T, Promise<T>> configure(Reactor reactor) {
			Promise<T> p = new Promise<T>(env, reactor, parent, null, null, null);
			final Deferred<T, Promise<T>> d = new Deferred<T, Promise<T>>(p);
			return d;
		}
	}

	public static class StreamSpec<T> extends ComponentSpec<StreamSpec<T>, Deferred<T, Stream<T>>> {
		private Composable<?> parent;
		private int batchSize = -1;
		private Iterable<T> values;

		public StreamSpec<T> link(Composable<?> parent) {
			this.parent = parent;
			return this;
		}

		public StreamSpec<T> batch(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public StreamSpec<T> each(Iterable<T> values) {
			this.values = values;
			return this;
		}

		@Override
		protected Deferred<T, Stream<T>> configure(Reactor reactor) {
			Stream<T> s = new Stream<T>(env, reactor, batchSize, values, parent);
			Deferred<T, Stream<T>> d = new Deferred<T, Stream<T>>(s);
			return d;
		}
	}

}
