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

import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ElapsedAction<T> extends Action<T, Tuple2<Long, T>> {

	private long lastTime;

	public ElapsedAction(Dispatcher dispatcher) {
		super(dispatcher);
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
		lastTime = System.nanoTime();
	}

	@Override
	protected void doNext(T ev) {
		long previousTime = lastTime;
		lastTime = System.nanoTime();

		broadcastNext(Tuple.of(lastTime - previousTime, ev));
	}
}
