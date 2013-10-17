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

import reactor.event.Event;
import reactor.function.Consumer;
import reactor.function.Predicate;
import reactor.queue.BlockingQueueFactory;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batches {@link reactor.event.Event Events} while the {@code queueWhile} {@link reactor.function.Predicate} returns
 * {@code true}. Flushes events when the {@code flushWhen} {@link reactor.function.Predicate} returns {@code true}.
 * Flushing occurs by notifying the given {@link reactor.core.Observable} using the given {@code key} and pulling
 * events
 * from the queue oldest to youngest.
 *
 * @author Jon Brisbin
 */
public class EventBatcher<T> implements Consumer<Event<T>> {

	private final AtomicLong count      = new AtomicLong();
	private final AtomicLong flushCount = new AtomicLong();
	private final Observable                 observable;
	private final Object                     key;
	private final Queue<Event<T>>            queue;
	private final Predicate<Queue<Event<T>>> queueWhile;
	private final Predicate<Queue<Event<T>>> flushWhen;

	public EventBatcher(@Nonnull Observable observable,
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

	/**
	 * Flush queued events by notifying the configured {@link reactor.core.Observable}.
	 */
	public void flush() {
		flushCount.set(count.get());
		Event<T> ev;
		while(flushCount.getAndDecrement() > 0 && null != (ev = queue.poll())) {
			observable.notify(key, ev);
		}
	}

	@Override
	public final void accept(Event<T> ev) {
		if(null == queueWhile || queueWhile.test(queue)) {
			queue.add(ev);
			count.incrementAndGet();
		}
		if(null != flushWhen && flushWhen.test(queue)) {
			flush();
		}
	}

}
