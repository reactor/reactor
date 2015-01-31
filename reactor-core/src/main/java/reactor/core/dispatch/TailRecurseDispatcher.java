/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.dispatch;

import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A {@link reactor.core.Dispatcher} implementation that trampolines events using the calling thread and.
 *
 * @author Stephane Maldini
 */
public final class TailRecurseDispatcher implements Dispatcher {

	public static final TailRecurseDispatcher INSTANCE = new TailRecurseDispatcher();

	private final        PriorityBlockingQueue<Task>                      queue           = new
			PriorityBlockingQueue<Task>();
	private final        AtomicInteger                                    wip             = new AtomicInteger();
	private static final AtomicIntegerFieldUpdater<TailRecurseDispatcher> COUNTER_UPDATER =
			AtomicIntegerFieldUpdater.newUpdater
					(TailRecurseDispatcher.class, "counter");

	private volatile boolean terminated = false;
	private volatile int counter;

	public TailRecurseDispatcher() {
	}

	@Override
	public boolean alive() {
		return terminated;
	}

	@Override
	public boolean awaitAndShutdown() {
		terminated = true;
		return true;
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		terminated = true;
		return true;
	}

	@Override
	public void shutdown() {
		awaitAndShutdown();
	}

	@Override
	public void forceShutdown() {
		awaitAndShutdown();
	}

	@Override
	public <E> void tryDispatch(E event,
	                            Consumer<E> consumer,
	                            Consumer<Throwable> errorConsumer) {
		dispatch(event, consumer, errorConsumer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E> void dispatch(E event,
	                         Consumer<E> eventConsumer,
	                         Consumer<Throwable> errorConsumer) {
		if (terminated) {
			return;
		}

		final Task task = new Task(COUNTER_UPDATER.incrementAndGet(this), event, eventConsumer, errorConsumer);

		queue.add(task);

		if (wip.getAndIncrement() == 0) {
			do {
				final Task polled = queue.poll();
				if (polled != null) {
					try {
						polled.eventConsumer.accept(polled.data);
					} catch (Throwable e) {
						if (polled.errorConsumer != null) {
							polled.errorConsumer.accept(e);
						} else if (Environment.alive()) {
							Environment.get().routeError(e);
						}
					}
				}
			} while (wip.decrementAndGet() > 0);
		}
	}

	@Override
	public void execute(final Runnable command) {
		dispatch(null, new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				command.run();
			}
		}, null);
	}

	@Override
	public long remainingSlots() {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean supportsOrdering() {
		return true;
	}

	@Override
	public int backlogSize() {
		return counter;
	}

	@Override
	public boolean inContext() {
		return true;
	}

	@Override
	public String toString() {
		return counter + ", " + queue.toString();
	}

	private static class Task implements Comparable<Task> {
		final Object              data;
		final Consumer            eventConsumer;
		final Consumer<Throwable> errorConsumer;
		final int                 index;

		public Task(int index, Object data, Consumer eventConsumer, Consumer<Throwable> errorConsumer) {
			this.data = data;
			this.index = index;
			this.eventConsumer = eventConsumer;
			this.errorConsumer = errorConsumer;
		}

		@Override
		public int compareTo(Task o) {
			return Integer.compare(index, o.index);
		}
	}
}
