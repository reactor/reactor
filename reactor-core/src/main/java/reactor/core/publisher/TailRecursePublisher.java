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
package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Function;

/**
 * A logging interceptor that intercepts all reactive calls and trace them
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class TailRecursePublisher<IN> implements Publisher<IN> {

	/**
	 * @param publisher
	 * @param category
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> log(Publisher<? extends IN> publisher, String category) {
		return new TailRecursePublisher<>(publisher, category);
	}


	private final Publisher<IN> wrappedPublisher;

	protected TailRecursePublisher(final Publisher<? extends IN> source,
	                               final String category) {

		final Logger log = category != null && !category.isEmpty() ?
		  LoggerFactory.getLogger(category) :
		  LoggerFactory.getLogger(TailRecursePublisher.class);

		this.wrappedPublisher = PublisherFactory.intercept(
		  source,
		  new Function<Subscriber<? super IN>, SubscriberBarrier<IN, IN>>() {
			  @Override
			  public SubscriberBarrier<IN, IN> apply(Subscriber<? super IN> subscriber) {
				  if (log.isTraceEnabled()) {
					  log.trace("subscribe: {}", subscriber.getClass().getSimpleName());
				  }
				  return new LoggerBarrier<>(log, subscriber);
			  }
		  });
	}

	@Override
	public void subscribe(Subscriber<? super IN> s) {
		wrappedPublisher.subscribe(s);
	}

	private static class LoggerBarrier<IN> extends SubscriberBarrier<IN, IN> {

		private final Logger log;

		public LoggerBarrier(Logger log,
		                     Subscriber<? super IN> subscriber) {
			super(subscriber);
			this.log = log;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			if (log.isInfoEnabled()) {
				log.info("⇩ onSubscribe({})", this.subscription);
			}
			super.doOnSubscribe(subscription);
		}

		@Override
		protected void doNext(IN in) {
			if (log.isInfoEnabled()) {
				log.info("↓ onNext({})", in);
			}
			super.doNext(in);
		}

		@Override
		protected void doError(Throwable throwable) {
			if (log.isErrorEnabled()) {
				log.error("↯ onError({})", throwable);
			}
			super.doError(throwable);
		}

		@Override
		protected void doComplete() {
			if (log.isInfoEnabled()) {
				log.info("↧ onComplete()");
			}
			super.doComplete();
		}

		@Override
		protected void doRequest(long n) {
			if (log.isInfoEnabled()) {
				log.info("⇡ request({})", n);
			}
			super.doRequest(n);
		}

		@Override
		protected void doCancel() {
			if (log.isInfoEnabled()) {
				log.info("↥ cancel()");
			}
			super.doCancel();
		}


		@Override
		public String toString() {
			return super.toString() + "{logger=" + log.getName() + "}";
		}
	}

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
	public long backlogSize() {
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
