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
package reactor.core.composable.action;

import reactor.event.dispatch.Dispatcher;
import reactor.function.Function;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class MapAction<T, V> extends Action<T, V> {

	private final Function<T, V> fn;

	public MapAction(Function<T, V> fn, Dispatcher dispatcher, ActionProcessor<V> d) {
		super(dispatcher, d);
		this.fn = fn;
	}

	@Override
	public void doNext(T value) {
		output.onNext(fn.apply(value));
		available();
	}
}
