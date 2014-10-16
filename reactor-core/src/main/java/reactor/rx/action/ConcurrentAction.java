/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package reactor.rx.action;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.action.support.NonBlocking;
import reactor.timer.Timer;
import reactor.util.Assert;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ConcurrentAction<I, O> extends Action<I, O> {

	private final int poolSize;
	private final AtomicInteger active = new AtomicInteger();
	private final ParallelAction[] publishers;
	private final ParallelSubscriber[] subscribers;
	private final AtomicBoolean started     = new AtomicBoolean();
	private final ReentrantLock requestLock = new ReentrantLock();

	private volatile int roundRobinIndex = 0;

	private       Registration<? extends Consumer<Long>>      consumerRegistration;
	private final Function<Stream<I>, Publisher<? extends O>> parallelStreamMapper;

	@SuppressWarnings("unchecked")
	public ConcurrentAction(Dispatcher parentDispatcher,
	                        Function<Stream<I>, Publisher<? extends O>> parallelStream,
	                        Supplier<Dispatcher> multiDispatcher,
	                        int poolSize) {
		super(parentDispatcher);
		Assert.state(poolSize > 0, "Must provide a strictly positive number of concurrent sub-streams (poolSize)");
		Assert.state(parallelStream != null, "Must define a concurrent stream with the Function<Stream,Stream> argument");
		this.poolSize = poolSize;
		this.parallelStreamMapper = parallelStream;
		this.subscribers = new ParallelSubscriber[poolSize];
		this.publishers = new ParallelAction[poolSize];
		for (int i = 0; i < poolSize; i++) {
			this.publishers[i] = new ParallelAction<I, O>(ConcurrentAction.this, multiDispatcher.get(), i);
		}
	}

	@Override
	public ConcurrentAction<I, O> capacity(long elements) {
		int cumulatedReservedSlots = poolSize * RESERVED_SLOTS;
		if (elements < cumulatedReservedSlots) {
			super.capacity(elements);
		} else {
			long newCapacity = elements - cumulatedReservedSlots + RESERVED_SLOTS;
			super.capacity(newCapacity);
		}
		long size = capacity / poolSize;

		if (size == 0) {
			size = elements;
		}

		for (ParallelAction p : publishers) {
			p.capacity(size);
		}
		return this;
	}

	/**
	 * Monitor all sub-streams latency to hint the next elements to dispatch to the fastest sub-streams in priority.
	 *
	 * @param latencyInMs a period in milliseconds to tolerate before assigning a new sub-stream
	 */
	public ConcurrentAction<I, O> monitorLatency(long latencyInMs) {
		Assert.isTrue(environment != null, "Require an environment to retrieve the default timer");
		return monitorLatency(latencyInMs, environment.getTimer());
	}

	/**
	 * Monitor all sub-streams latency to hint the next elements to dispatch to the fastest sub-streams in priority.
	 *
	 * @param latencyInMs a period in milliseconds to tolerate before assigning a new sub-stream
	 * @param timer       a timer to run on periodically
	 */
	public ConcurrentAction<I, O> monitorLatency(final long latencyInMs, Timer timer) {
		consumerRegistration = timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				trySyncDispatch(aLong, new Consumer<Long>() {
					@Override
					public void accept(Long aLong) {

						try {
							if (aLong - publishers[roundRobinIndex].getLastRequestedTime() < latencyInMs) return;
						} catch (NullPointerException npe) {
							//ignore
						}

						int fasterParallelIndex = -1;
						for (ParallelAction parallelStream : publishers) {
							try {
								if (aLong - parallelStream.getLastRequestedTime() < latencyInMs) {
									fasterParallelIndex = parallelStream.getIndex();
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

	public ParallelAction[] getPublishers() {
		return publishers;
	}

	@Override
	public ConcurrentAction<I, O> env(Environment environment) {
		for (ParallelAction p : publishers) {
			p.env(environment);
		}
		super.env(environment);
		return this;
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		if(started.compareAndSet(false, true)){
			startNewPublisher();
		}
		super.subscribe(subscriber);
	}

	@Override
	protected void requestUpstream(final AtomicLong capacity, final boolean terminated, final long elements) {
		trySyncDispatch(elements, new Consumer<Long>() {
			@Override
			public void accept(Long elements) {
				long actives = active.get();
				if(actives <= 0){
					ConcurrentAction.super.requestUpstream(capacity, terminated, elements);
				} else {
					long downstreamCapacity = elements;
					long toRequest = downstreamCapacity / actives;
					toRequest += downstreamCapacity % actives;
					if(toRequest > 0){
						int i = 0;
						while(downstreamCapacity > 0){
							if(subscribers[i] != null){
								downstreamCapacity -= toRequest;
								subscribers[i].request(toRequest);
							}
							if(++i > actives){
								i = 0;
							}
						}
					}
				}
			}
		});

	}

	@SuppressWarnings("unchecked")
	private ParallelAction<I, O> startNewPublisher() {
		final int actives = active.getAndIncrement();
		if(actives < poolSize) {
			ParallelAction<I, O> parallelAction = publishers[actives];
			Publisher<? extends O> tail = parallelStreamMapper.apply(parallelAction);
			subscribers[actives] = new ParallelSubscriber<O>(this, actives);
			tail.subscribe(subscribers[actives]);

			if(actives == 0 || downstreamSubscription == null) return parallelAction;

			trySyncDispatch(null, new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					if(downstreamSubscription != null && downstreamSubscription.pendingRequestSignals() > 0){
						long downstreamPending = downstreamSubscription.pendingRequestSignals();
						if(downstreamPending == Long.MAX_VALUE) {
							subscribers[actives].request(downstreamPending);
						}else{
							long toRequest = downstreamPending / actives;
							toRequest += downstreamPending % actives;
							if(toRequest > 0){
								subscribers[actives].request(toRequest);
							}
						}
					}
				}
			});


			return parallelAction;
		}else{
			active.decrementAndGet();
		}
		return null;
	}

	void cleanPublisher() {
		if (active.decrementAndGet() <= 0) {
			cancel();
		}
	}

	void parallelRequest(long elements) {
		requestLock.lock();
		try {
			onRequest(elements);
		} finally {
			requestLock.unlock();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(final I ev) {

		ParallelAction<I, O> publisher;
		boolean hasCapacity;
		int tries = 0;
		int lastExistingPublisher = -1;
		int currentRoundRobIndex = roundRobinIndex;

		while (tries < poolSize) {
			publisher = publishers[currentRoundRobIndex];

			if (publisher != null) {
				lastExistingPublisher = currentRoundRobIndex;

				hasCapacity = publisher.hasCapacity();

				if (hasCapacity) {
					publisher.broadcastNext(ev);
					return;
				} else {
					publisher = startNewPublisher();
					if (publisher != null){
						publisher.broadcastNext(ev);
						return;
					}
				}
			}

			if (++currentRoundRobIndex >= poolSize) {
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
		} /*else {
			if (log.isTraceEnabled()) {
				log.trace("event dropped " + ev + " as downstream publisher is shutdown");
			}
		}*/

	}

	protected void onShutdown() {
		dispatch(new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				if (active.get() == 0) {
					cancel();
				}
			}
		});
	}

	@Override
	public void broadcastComplete() {
		if(active.decrementAndGet() < 0){
			super.broadcastComplete();
		} else {
			active.incrementAndGet();
		}
	}

	@Override
	protected void doError(Throwable throwable) {
		if (consumerRegistration != null) consumerRegistration.cancel();
		for (ParallelAction parallelStream : publishers) {
			if(parallelStream != null) parallelStream.broadcastError(throwable);
		}
	}

	@Override
	protected void doComplete() {
		if (consumerRegistration != null) consumerRegistration.cancel();
		for (ParallelAction parallelStream : publishers) {
			if(parallelStream != null) parallelStream.broadcastComplete();
		}
	}

	private final static class ParallelSubscriber<O> implements NonBlocking, Subscriber<O>, Subscription, Consumer<O>{

		final ConcurrentAction<?, O> outputAction;
		final int index;

		long requested = 0;

		Subscription subscription;

		private ParallelSubscriber(ConcurrentAction<?, O> outputAction, int index) {
			this.outputAction = outputAction;
			this.index = index;
		}

		@Override
		public void onSubscribe(Subscription s) {
			subscription = s;
		}

		@Override
		public void onNext(O o) {
			outputAction.trySyncDispatch(o, this);
		}

		public void accept(O o){
			requested--;
			outputAction.broadcastNext(o);
		}

		@Override
		public void onError(Throwable t) {
			outputAction.broadcastError(t);
		}

		@Override
		public void onComplete() {
			outputAction.broadcastComplete();
		}

		@Override
		public void request(long n) {
			if(requested < Long.MAX_VALUE) {
				if ((requested += n) < 0) requested = Long.MAX_VALUE;
				subscription.request(n);
			}
		}

		@Override
		public void cancel() {
			outputAction.subscribers[index] = null;
			subscription.cancel();
		}

		@Override
		public Dispatcher getDispatcher() {
			return outputAction.dispatcher;
		}

		@Override
		public long getCapacity() {
			return outputAction.capacity;
		}
	}
}
