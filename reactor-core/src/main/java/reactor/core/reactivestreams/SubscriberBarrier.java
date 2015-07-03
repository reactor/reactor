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
package reactor.core.reactivestreams;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.support.Exceptions;
import reactor.core.support.NonBlocking;
import reactor.core.support.SpecificationExceptions;

/**
 * A {@link Subscriber} with an asymetric typed wrapped subscriber. Yet it represents a unique relationship between
 * a Publisher and a Subscriber, it doesn't implement
 * the {@link org.reactivestreams.Processor} interface allowing multiple subscribes.
 *
 * @author Stephane Maldini
 * @since 2.0.4
 */
public class SubscriberBarrier<I, O> implements Subscriber<I>, Subscription, NonBlocking {

	protected final Subscriber<? super O> subscriber;

	private Subscription subscription;

	public SubscriberBarrier(Subscriber<? super O> subscriber) {
		this.subscriber = subscriber;
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (s == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}

		try {
			if (subscription != null) {
				s.cancel();
				return;
			}
			subscription = s;
			doSubscribe(this);
		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			doError(throwable);
		}
	}

	protected void doSubscribe(Subscription subscription) {
		subscriber.onSubscribe(subscription);
	}

	@Override
	public final void onNext(I i) {
		try {
			doNext(i);
		} catch (Throwable throwable) {
			doError(throwable);
		}
	}

	@SuppressWarnings("unchecked")
	protected void doNext(I i) {
		subscriber.onNext((O) i);
	}

	@Override
	public final void onError(Throwable t) {
		doError(t);
	}

	protected void doError(Throwable throwable) {
		subscriber.onError(throwable);
	}

	@Override
	public final void onComplete() {
		try {
			doComplete();
		} catch (Throwable throwable) {
			doError(throwable);
		}
	}

	protected void doComplete() {
		subscriber.onComplete();
	}

	@Override
	public final void request(long n) {
		if(n < 0) throw SpecificationExceptions.spec_3_09_exception(n);
		try {
			doRequest(n);
		} catch (Throwable throwable) {
			doError(throwable);
		}
	}

	protected void doRequest(long n) {
		Subscription s = this.subscription;
		if (s != null) {
			s.request(n);
		}
	}

	@Override
	public final void cancel() {
		try {
			doCancel();
		} catch (Throwable throwable) {
			doError(throwable);
		}
	}

	protected void doCancel() {
		Subscription s = this.subscription;
		if (s != null) {
			this.subscription = null;
			s.cancel();
		}
	}


	@Override
	public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
		return NonBlocking.class.isAssignableFrom(subscriber.getClass())
				&& ((NonBlocking) subscriber).isReactivePull(dispatcher, producerCapacity);
	}

	@Override
	public long getCapacity() {
		return NonBlocking.class.isAssignableFrom(subscriber.getClass()) ?
				((NonBlocking) subscriber).getCapacity() :
				Long.MAX_VALUE;
	}
}
