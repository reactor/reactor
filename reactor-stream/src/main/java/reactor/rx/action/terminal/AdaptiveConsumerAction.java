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
package reactor.rx.action.terminal;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.core.support.Bounded;
import reactor.core.support.Exceptions;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author Stephane Maldini
 */
public final class AdaptiveConsumerAction<T> extends Action<T, Void> {

	private final Consumer<? super T> consumer;
	private final Dispatcher          dispatcher;
	private final Broadcaster<Long>   requestMapperStream;

	private final AtomicLongFieldUpdater<AdaptiveConsumerAction> COUNTED = AtomicLongFieldUpdater.newUpdater
			(AdaptiveConsumerAction
					.class, "counted");

	private final AtomicLongFieldUpdater<AdaptiveConsumerAction> COUNTING = AtomicLongFieldUpdater.newUpdater
			(AdaptiveConsumerAction
					.class, "counting");

	private volatile long counted;
	private volatile long counting;
	private          long pendingRequests;


	public AdaptiveConsumerAction(Dispatcher dispatcher,
	                              long initCapacity,
	                              Consumer<? super T> consumer,
	                              Function<Stream<Long>, ? extends Publisher<? extends Long>>
			                              requestMapper) {
		this.consumer = consumer;
		this.requestMapperStream = Broadcaster.create();
		this.requestMapperStream.onSubscribe(new Subscription() {
			@Override
			public void request(long n) {
				//IGNORE
			}

			@Override
			public void cancel() {
				Subscription subscription = upstreamSubscription;
				if(subscription != null){
					upstreamSubscription = null;
					subscription.cancel();
				}
			}
		});
		if (SynchronousDispatcher.INSTANCE == dispatcher) {
			this.dispatcher =  TailRecurseDispatcher.INSTANCE;
		} else {
			this.dispatcher = dispatcher;
		}
		//TODO define option to choose ?
		this.capacity = initCapacity;

		Publisher<? extends Long> afterRequestStream = requestMapper.apply(requestMapperStream);
		afterRequestStream.subscribe(new RequestSubscriber());
	}

	@Override
	public void requestMore(long n) {
		if (upstreamSubscription != null) {
			requestMapperStream.onNext(n);
		} else {
			synchronized (this) {
				if ((pendingRequests += n) < 0l) {
					pendingRequests = Long.MAX_VALUE;
				}
			}
		}
	}

	@Override
	protected void doNext(T ev) {
		if (consumer != null) {
			consumer.accept(ev);
		}

		COUNTING.incrementAndGet(this);

		if (upstreamSubscription != null
				&& capacity != Long.MAX_VALUE
				&& COUNTED.decrementAndGet(this) == 0) {
			requestMore(COUNTING.getAndSet(this, 0l));
		}
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		long toRequest;

		synchronized (this) {
			toRequest = pendingRequests;
			pendingRequests = 0l;
		}

		if (toRequest > 0l) {
			requestMore(toRequest);
		}
	}

	@Override
	protected void doError(Throwable ev) {
		cancel();
		requestMapperStream.onError(ev);
		super.doError(ev);
	}

	@Override
	protected void doShutdown() {
		cancel();
		requestMapperStream.onComplete();
		super.doShutdown();
	}

	@Override
	protected PushSubscription<Void> createSubscription(Subscriber<? super Void> subscriber, boolean reactivePull) {
		return new PushSubscription<Void>(this, subscriber) {
			@Override
			public void request(long n) {
				//IGNORE
			}
		};
	}

	@Override
	public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
		return capacity != Long.MAX_VALUE;
	}

	@Override
	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	public String toString() {
		return super.toString() + "{pending=" + pendingRequests + "}";
	}

	private class RequestSubscriber implements Subscriber<Long>, Bounded {
		Subscription s;

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			s.request(1l);
		}

		@Override
		public void onNext(Long n) {
			if (COUNTED.addAndGet(AdaptiveConsumerAction.this, n) < 0l) {
				COUNTED.set(AdaptiveConsumerAction.this, Long.MAX_VALUE);
			}
			PushSubscription<T> upstreamSubscription = AdaptiveConsumerAction.this.upstreamSubscription;
			if(upstreamSubscription != null) {
				TailRecurseDispatcher.INSTANCE.dispatch(n, upstreamSubscription, null);
			}
			if (s != null) {
				s.request(1l);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (s != null) {
				s.cancel();
			}
			Exceptions.throwIfFatal(t);
		}

		@Override
		public void onComplete() {
			if (s != null) {
				s.cancel();
			}
		}

		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return AdaptiveConsumerAction.this.isReactivePull(dispatcher, producerCapacity);
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

	}
}
