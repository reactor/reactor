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

import java.util.Queue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import reactor.event.Event;
import reactor.function.Consumer;
import reactor.function.Observable;
import reactor.function.Predicate;
import reactor.queue.BlockingQueueFactory;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class Sequencer<T> implements Consumer<Event<T>> {

	private final Observable                 observable;
	private final Object                     key;
	private final Queue<Event<T>>            queue;
	private final Predicate<Queue<Event<T>>> queueWhile;
	private final Predicate<Queue<Event<T>>> flushWhen;

	public Sequencer(@Nonnull Observable observable,
	                 @Nonnull Object key,
	                 @Nullable Queue<Event<T>> queue,
	                 @Nullable Predicate<Queue<Event<T>>> queueWhile,
	                 @Nullable Predicate<Queue<Event<T>>> flushWhen) {
		Assert.notNull(observable, "Reactor cannot be null.");
		Assert.notNull(key, "Event key cannot be null.");
		this.observable = observable;
		this.key = key;
		this.queue = (null == queue ? BlockingQueueFactory.<Event<T>>createQueue() : queue);
		this.queueWhile = queueWhile;
		this.flushWhen = flushWhen;
	}

	public void flush() {
		Event<T> ev;
		while(null != (ev = queue.poll())) {
			observable.notify(key, ev);
		}
	}

	@Override
	public final void accept(Event<T> ev) {
		if(null == queueWhile || queueWhile.test(queue)) {
			queue.add(ev);
		}
		if(null != flushWhen && flushWhen.test(queue)) {
			flush();
		}
	}

}
