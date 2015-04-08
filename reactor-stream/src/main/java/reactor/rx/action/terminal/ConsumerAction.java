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
package reactor.rx.action.terminal;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author Stephane Maldini
 */
public final class ConsumerAction<T> extends Action<T, Void> {

	private final Consumer<? super T>         consumer;
	private final Consumer<? super Throwable> errorConsumer;
	private final Consumer<Void>              completeConsumer;
	private final Dispatcher                  dispatcher;

	private final AtomicLongFieldUpdater<ConsumerAction> COUNTED = AtomicLongFieldUpdater.newUpdater(ConsumerAction
			.class, "count");

	private volatile long count;
	private          long pendingRequests;


	public ConsumerAction(Dispatcher dispatcher, Consumer<? super T> consumer,
	                      Consumer<? super Throwable> errorConsumer, Consumer<Void> completeConsumer) {
		this.consumer = consumer;
		this.dispatcher = dispatcher == SynchronousDispatcher.INSTANCE ? TailRecurseDispatcher.INSTANCE : dispatcher;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;

		//TODO define option to choose ?
		this.capacity = Long.MAX_VALUE;


		if (consumer != null) {
			pendingRequests = capacity;
		}
	}

	@Override
	public void requestMore(long n) {
		PushSubscription<T> upstreamSubscription = this.upstreamSubscription;
		if (upstreamSubscription != null) {
			long toRequest = Math.min(n, capacity);
			if(COUNTED.addAndGet(this, toRequest) < 0l){
				COUNTED.set(this, Long.MAX_VALUE);
			}
			dispatcher.dispatch(toRequest, upstreamSubscription, null);
		}else{
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
		if (upstreamSubscription != null
				&& capacity != Long.MAX_VALUE
				&& COUNTED.decrementAndGet(this) == 0) {
			requestMore(capacity);
		}
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		long toRequest;
		synchronized (this){
			toRequest = pendingRequests;
			pendingRequests = 0l;
		}
		if(toRequest > 0l){
			requestMore(toRequest);
		}
	}

	@Override
	protected void doError(Throwable ev) {
		if (errorConsumer != null) {
			errorConsumer.accept(ev);
		}
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		if (completeConsumer != null) {
			completeConsumer.accept(null);
		}
		super.doComplete();
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
}
