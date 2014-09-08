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

import org.reactivestreams.Subscriber;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;
import reactor.timer.Timer;
import reactor.util.Assert;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ParallelAction<O> extends Action<O, Stream<O>> {

	private final ParallelStream[] publishers;
	private final int              poolSize;
	private final ReentrantLock lock   = new ReentrantLock();
	private final AtomicInteger active = new AtomicInteger();

	private volatile int roundRobinIndex = 0;

	private Registration<? extends Consumer<Long>> consumerRegistration;

	@SuppressWarnings("unchecked")
	public ParallelAction(Dispatcher parentDispatcher,
	                      Supplier<Dispatcher> multiDispatcher,
	                      Integer poolSize) {
		super(parentDispatcher);
		Assert.state(poolSize > 0, "Must provide a strictly positive number of concurrent sub-streams (poolSize)");
		this.poolSize = poolSize;
		this.publishers = new ParallelStream[poolSize];
		for (int i = 0; i < poolSize; i++) {
			this.publishers[i] = new ParallelStream<O>(ParallelAction.this, multiDispatcher.get(), i);
		}
	}

	@Override
	public Action<O, Stream<O>> capacity(long elements) {
		int cumulatedReservedSlots = poolSize * RESERVED_SLOTS;
		if (elements < cumulatedReservedSlots) {
			log.warn("So, because we try to book some {} slots on the parallel master action and " +
							"we need at least {} slots to never overrun the underlying dispatchers, we decided to" +
							" leave the parallel master action capacity to {}", elements,
					cumulatedReservedSlots, elements);
			super.capacity(elements);
		} else {
			super.capacity(elements - cumulatedReservedSlots + RESERVED_SLOTS);
		}
		long size = capacity / poolSize;

		if (size == 0) {
			log.warn("Of course there are {} parallel streams and there can only be {} max items available at any given " +
							"time, " +
							"we baselined all parallel streams capacity to {}",
					poolSize, elements, elements);
			size = elements;
		}

		for (ParallelStream p : publishers) {
			p.capacity(size);
		}
		return this;
	}

	@Override
	public Action<O, Stream<O>> env(Environment environment) {
		for (ParallelStream p : publishers) {
			p.env(environment);
		}
		return super.env(environment);
	}

	@Override
	public Action<O, Stream<O>> keepAlive(boolean keepAlive) {
		super.keepAlive(keepAlive);
		for (ParallelStream p : publishers) {
			p.keepAlive(keepAlive);
		}
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected StreamSubscription<Stream<O>> createSubscription(final Subscriber<? super Stream<O>> subscriber) {
		return new StreamSubscription.Firehose<Stream<O>>(this, subscriber) {
			long cursor = 0l;

			@Override
			public void request(long elements) {
				int i = 0;
				while (i < poolSize && i < cursor) {
					i++;
				}

				while (i < elements && i < poolSize) {
					cursor++;
					active.incrementAndGet();
					onNext(publishers[i]);
					i++;
				}

				if (i == poolSize) {
					onComplete();
				}
			}
		};
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(final O ev) {

		ParallelStream<O> publisher;
		boolean hasCapacity;
		int tries = 0;
		int lastExistingPublisher = -1;
		int currentRoundRobIndex = roundRobinIndex;

		while (tries < poolSize) {
			publisher = publishers[currentRoundRobIndex];

			if (publisher != null) {
				lastExistingPublisher = currentRoundRobIndex;

				hasCapacity = publisher.downstreamSubscription() != null &&
						publisher.downstreamSubscription().getCapacity().get() > publisher.getMaxCapacity() * 0.15;

				if (hasCapacity) {
					try {
						publisher.broadcastNext(ev);
					} catch (Throwable e) {
						publisher.broadcastError(e);
					}
					return;
				}
			}

			if (++currentRoundRobIndex == poolSize) {
				currentRoundRobIndex = 0;
			}

			tries++;
		}

		if (lastExistingPublisher != -1) {
			roundRobinIndex = lastExistingPublisher;
			publisher = publishers[lastExistingPublisher];
			try {
				publisher.broadcastNext(ev);
			} catch (Throwable e) {
				publisher.broadcastError(e);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("event dropped " + ev + " as downstream publisher is shutdown");
			}
		}

	}

	@Override
	protected void doError(Throwable throwable) {
		super.doError(throwable);
		if (consumerRegistration != null) consumerRegistration.cancel();
		for (ParallelStream parallelStream : publishers) {
			parallelStream.broadcastError(throwable);
		}
	}


	@Override
	protected void doComplete() {
		super.doComplete();
		if (consumerRegistration != null) consumerRegistration.cancel();
		for (ParallelStream parallelStream : publishers) {
			parallelStream.broadcastComplete();
		}
	}

	/**
	 * Monitor all sub-streams latency to hint the next elements to dispatch to the fastest sub-streams in priority.
	 *
	 * @param latencyInMs a period in milliseconds to tolerate before assigning a new sub-stream
	 */
	public ParallelAction<O> monitorLatency(long latencyInMs) {
		Assert.isTrue(environment != null, "Require an environment to retrieve the default timer");
		return monitorLatency(latencyInMs, environment.getRootTimer());
	}

	/**
	 * Monitor all sub-streams latency to hint the next elements to dispatch to the fastest sub-streams in priority.
	 *
	 * @param latencyInMs a period in milliseconds to tolerate before assigning a new sub-stream
	 * @param timer       a timer to run on periodically
	 */
	public ParallelAction<O> monitorLatency(final long latencyInMs, Timer timer) {
		consumerRegistration = timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				trySyncDispatch(aLong, new Consumer<Long>() {
					@Override
					public void accept(Long aLong) {

						try {
							if (aLong - publishers[roundRobinIndex].lastRequestedTime < latencyInMs) return;
						} catch (NullPointerException npe) {
							//ignore
						}

						int fasterParallelIndex = -1;
						for (ParallelStream parallelStream : publishers) {
							try {
								if (aLong - parallelStream.lastRequestedTime < latencyInMs) {
									fasterParallelIndex = parallelStream.index;
									break;
								}
							} catch (NullPointerException npe) {
								//ignore
							}
						}

						if (fasterParallelIndex == -1) return;

						roundRobinIndex = fasterParallelIndex;
					}
				});
			}
		}, latencyInMs, TimeUnit.MILLISECONDS, latencyInMs);
		return this;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public ParallelStream[] getPublishers() {
		return publishers;
	}

	static private class ParallelStream<O> extends Stream<O> {
		final ParallelAction<O> parallelAction;
		final int               index;

		volatile long lastRequestedTime = -1l;

		private ParallelStream(ParallelAction<O> parallelAction, Dispatcher dispatcher, int index) {
			super(dispatcher);
			this.parallelAction = parallelAction;
			this.index = index;
		}

		@Override
		public void broadcastComplete() {
			dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					ParallelStream.super.broadcastComplete();
				}
			});
		}

		@Override
		public void broadcastError(Throwable throwable) {
			dispatch(throwable, new Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					ParallelStream.super.broadcastError(throwable);
				}
			});
		}

		@Override
		protected StreamSubscription<O> createSubscription(Subscriber<? super O> subscriber) {
			return new StreamSubscription<O>(this, subscriber) {
				@Override
				public void request(long elements) {
					super.request(elements);
					lastRequestedTime = System.currentTimeMillis();

					parallelAction.lock.lock();
					try {
						parallelAction.roundRobinIndex = index;
						parallelAction.requestConsumer.accept(elements);
					}finally {
						parallelAction.lock.unlock();
					}
				}

				@Override
				public void cancel() {
					super.cancel();
					parallelAction.publishers[index] = null;

					if(parallelAction.active.decrementAndGet() <= 0){
						parallelAction.cancel();
					}
				}

			};
		}

		@Override
		public String toString() {
			return super.toString() + "{" + (index + 1) + "/" + parallelAction.poolSize + "}";
		}
	}
}
