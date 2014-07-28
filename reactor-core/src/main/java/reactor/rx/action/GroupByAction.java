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
package reactor.rx.action;

import reactor.event.dispatch.Dispatcher;
import reactor.function.Function;
import reactor.rx.Stream;
import reactor.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class GroupByAction<T, K> extends Action<T, Stream<T>> {

	private final Function<T, K> fn;
	private final Map<K, Stream<T>> groupByMap = new HashMap<K, Stream<T>>();

	public GroupByAction(Function<T, K> fn, Dispatcher dispatcher) {
		super(dispatcher);
		Assert.notNull(fn, "Key mapping function cannot be null.");
		this.fn = fn;
	}

	public Map<K, Stream<T>> groupByMap() {
		return groupByMap;
	}

	@Override
	protected void doNext(T value) {
		K key = fn.apply(value);
		Stream<T> stream = groupByMap.get(key);
		if (stream == null) {
			stream = new Stream<T>(dispatcher).capacity(batchSize).env(environment);
			stream.setKeepAlive(false);
			groupByMap.put(key, stream);
			broadcastNext(stream);
		}
		stream.broadcastNext(value);
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		for(Stream<T> stream : groupByMap.values()){
			stream.broadcastError(ev);
		}
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		for(Stream<T> stream : groupByMap.values()){
			stream.broadcastComplete();
		}
	}
}
