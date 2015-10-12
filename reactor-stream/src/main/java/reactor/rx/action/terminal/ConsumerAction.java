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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;

/**
 * @author Stephane Maldini
 */
public final class ConsumerAction<T> extends Action<T, Void> {

	private final Consumer<? super T>         consumer;
	private final Consumer<? super Throwable> errorConsumer;
	private final Consumer<Void>              completeConsumer;

	private final AtomicLongFieldUpdater<ConsumerAction> COUNTED = AtomicLongFieldUpdater.newUpdater(ConsumerAction
	  .class, "count");

	private volatile long count;
	private          long pendingRequests;


	public ConsumerAction(long capacity, Consumer<? super T> consumer,
	                      Consumer<? super Throwable> errorConsumer, Consumer<Void> completeConsumer) {
		this.consumer = consumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;

		//TODO define option to choose ?
		this.capacity = capacity;


		if (consumer != null) {
			pendingRequests = capacity;
		}
	}

	@Override
	public void requestMore(long n) {
		Subscription upstreamSubscription = this.upstreamSubscription;
		if (upstreamSubscription != null) {
			long toRequest = Math.min(n, capacity);
			if (COUNTED.addAndGet(this, toRequest) < 0l) {
				COUNTED.set(this, Long.MAX_VALUE);
			}
			upstreamSubscription.request(toRequest);
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
		if (upstreamSubscription != null
		  && capacity != Long.MAX_VALUE
				&& COUNTED.decrementAndGet(this) == 0) {
			requestMore(capacity);
		}
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
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
		recycle();
		if (errorConsumer != null) {
			errorConsumer.accept(ev);
		}else{
			Exceptions.throwIfFatal(ev);
			throw ReactorFatalException.create(ev);
		}
	}

	@Override
	protected void doComplete() {
		recycle();
		if (completeConsumer != null) {
			completeConsumer.accept(null);
		}
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
	public String toString() {
		return super.toString() + "{pending=" + pendingRequests + "}";
	}
}
