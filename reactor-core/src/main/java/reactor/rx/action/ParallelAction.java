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

import org.reactivestreams.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.rx.action.support.NonBlocking;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * A stream emitted from the {@link reactor.rx.Stream#parallel(reactor.function.Function)} action that tracks its position in the hosting
 * {@link reactor.rx.action.ConcurrentAction}. It also retains the last time it requested anything for
 * latency monitoring {@link reactor.rx.action.ConcurrentAction#monitorLatency(long)}.
 * The Stream will complete or fail whever the parent parallel action terminates itself or when broadcastXXX is called.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.parallel(8).consume(parallelStream -> parallelStream.consume())
 * }
 *
 * @author Stephane Maldini
 */
public final class ParallelAction<I, O> extends Action<I, I> {
	private final ConcurrentAction<I, O> parallelAction;
	private final int                 index;

	private long minimumToleratedCapacity = 1;

	private volatile long lastRequestedTime = -1l;
	private ReactiveSubscription<I> reactiveSubscription;

	public ParallelAction(ConcurrentAction<I, O> parallelAction, Dispatcher dispatcher, int index) {
		super(dispatcher);
		this.parallelAction = parallelAction;
		this.index = index;
		this.minimumToleratedCapacity = Math.max(1l, (long)(dispatcher.backlogSize() * 0.15));
	}

	@Override
	public Action<I, I> capacity(long elements) {
		super.capacity(elements);
		if(reactiveSubscription != null){
			reactiveSubscription.maxCapacity(elements);
		}
		this.minimumToleratedCapacity = Math.max(1l, (long)(elements * 0.15));
		return this;
	}



	public boolean hasCapacity() {
		if(reactiveSubscription == null){
			return false;
		}else{
			return reactiveSubscription.capacity().get() > minimumToleratedCapacity;
		}
	}

	@Override
	protected void doNext(I ev) {
		broadcastNext(ev);
	}

	public long getLastRequestedTime() {
		return lastRequestedTime;
	}

	public int getIndex() {
		return index;
	}

	@Override
	protected ReactiveSubscription<I> createSubscription(Subscriber<? super I> subscriber, boolean reactivePull) {
		reactiveSubscription = new ReactiveSubscription<I>(this, subscriber) {
			@Override
			public void request(long elements) {
				super.request(elements);
				lastRequestedTime = System.currentTimeMillis();
				parallelAction.parallelRequest(elements);
			}

			@Override
			public void cancel() {
				super.cancel();
				parallelAction.cleanPublisher();
			}

		};
		return reactiveSubscription;
	}

	@Override
	public void subscribe(Subscriber<? super I> subscriber) {
		try {
			final NonBlocking asyncSubscriber = NonBlocking.class.isAssignableFrom(subscriber.getClass()) ?
					(NonBlocking) subscriber :
					null;

			final PushSubscription<I> subscription = createSubscription(subscriber,true);

			if (null != asyncSubscriber) {
				subscription.maxCapacity(capacity);
			}

			subscribeWithSubscription(subscriber, subscription, false);

		} catch (Exception e) {
			subscriber.onError(e);
		}
	}

	@Override
	public String toString() {
		return super.toString() + "{" + (index + 1) + "/" + parallelAction.getPoolSize() + "}";
	}
}
