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
import reactor.core.composable.BufferedStream;
import reactor.core.composable.Composable;
import reactor.core.composable.Deferred;
import reactor.core.composable.Stream;
import reactor.event.selector.Selector;
import reactor.tuple.Tuple2;

/**
 * A helper class for specifying a {@link Deferred} {@link Stream}.
 *
 * @param <T> The type of values that the stream will contain
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class DeferredStreamSpec<T> extends ComposableSpec<DeferredStreamSpec<T>, Deferred<T, Stream<T>>> {

	private Composable<?> parent;
	private int batchSize = -1;
	private Iterable<T> values;

	/**
	 * Configures the stream to have the given {@code parent}. The default configuration is
	 * for the stream to have no parent. The stream will consume errors from its parent.
	 *
	 * @param parent The parent of the stream.
	 *
	 * @return {@code this}
	 */
	public DeferredStreamSpec<T> link(Composable<?> parent) {
		this.parent = parent;
		return this;
	}

	/**
	 * Configures the stream to have the given {@code batchSize}. A value of {@code -1}, which
	 * is the default configuration, configures the stream to not be batched.
	 *
	 * @param batchSize The batch size of the stream
	 *
	 * @return {@code this}
	 */
	public DeferredStreamSpec<T> batchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Configures the stream to contain the given initial {@code values}.
	 *
	 * @param values The stream's initial values
	 *
	 * @return {@code this}
	 */
	public DeferredStreamSpec<T> each(Iterable<T> values) {
		this.values = values;
		return this;
	}

	@Override
	protected Deferred<T, Stream<T>> createComposable(Environment env, Observable observable,
	                                                  Tuple2<Selector, Object> accept) {
		Stream<T> stream = values == null && batchSize < 1?
				new Stream<T>(observable, parent, accept, env) :
				new BufferedStream<T>(observable, batchSize, values, parent, accept, env);
		return new Deferred<T, Stream<T>>(stream);
	}

}
