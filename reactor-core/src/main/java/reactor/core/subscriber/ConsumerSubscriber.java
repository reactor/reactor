/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.subscriber;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.publisher.PublisherFactory;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public class ConsumerSubscriber<T> extends BaseSubscriber<T> implements ReactiveState.Upstream<T> {

	private final Consumer<? super T>         consumer;
	private final Consumer<? super Throwable> errorConsumer;
	private final Consumer<Void>              completeConsumer;

	private Subscription subscription;

	/**
	 *
	 */
	public ConsumerSubscriber(){
		this(null, null, null);
	}

	/**
	 *
	 * @param consumer
	 * @param errorConsumer
	 * @param completeConsumer
	 */
	public ConsumerSubscriber(Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Consumer<Void> completeConsumer) {
		this.consumer = consumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
	}

	/**
	 *
	 * @param s
	 */
	protected void doSubscribe(Subscription s) {
		s.request(Long.MAX_VALUE);
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (BackpressureUtils.checkSubscription(subscription, s)) {
			this.subscription = s;
			try {
				doSubscribe(s);
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				onError(t);
			}
		}
	}

	@Override
	public Publisher<T> upstream() {
		return PublisherFactory.fromSubscription(subscription);
	}

	@Override
	public final void onComplete() {
		Subscription s = subscription;
		if (s == null) {
			return;
		}
		subscription = null;
		try {
			doComplete();
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			doError(t);
		}
	}

	/**
	 *
	 */
	protected void doComplete() {
		if (completeConsumer != null) {
			completeConsumer.accept(null);
		}
	}

	@Override
	public final void onError(Throwable t) {
		super.onError(t);
		Subscription s = subscription;
		if (s == null) {
			return;
		}
		subscription = null;
		doError(t);
	}

	/**
	 *
	 * @param t
	 */
	protected void doError(Throwable t) {
		if (errorConsumer != null) {
			errorConsumer.accept(t);
		}
		else {
			throw ReactorFatalException.create(t);
		}
	}

	@Override
	public final void onNext(T x) {
		super.onNext(x);

		try {
			doNext(x);
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			Subscription s = subscription;
			if (s != null) {
				subscription = null;
				s.cancel();
			}
			doError(Exceptions.addValueAsLastCause(t, x));
		}
	}

	/**
	 *
	 * @param x
	 */
	protected void doNext(T x) {
		if (consumer != null) {
			consumer.accept(x);
		}
	}

	/**
	 *
	 * @param n
	 */
	protected void requestMore(long n){
		Subscription s = subscription;
		if (s != null) {
			s.request(n);
		}
	}

	/**
	 *
	 */
	protected void cancel() {
		Subscription s = subscription;
		if (s != null) {
			subscription = null;
			s.cancel();
		}
	}
}
