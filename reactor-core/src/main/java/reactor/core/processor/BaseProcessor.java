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

package reactor.core.processor;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;

/**
 * A base processor with an async boundary trait to manage active subscribers (Threads), upstream subscription and
 * shutdown options.
 * @author Stephane Maldini
 */
public abstract class BaseProcessor<IN, OUT> extends BaseSubscriber<IN>
		implements Processor<IN, OUT>, ReactiveState.Bounded, ReactiveState.Upstream {

	//protected static final int DEFAULT_BUFFER_SIZE = 1024;

	public static final int XS_BUFFER_SIZE     = 32;
	public static final int SMALL_BUFFER_SIZE  = 256;
	public static final int MEDIUM_BUFFER_SIZE = 8192;

	protected final boolean autoCancel;

	protected Subscription upstreamSubscription;

	protected BaseProcessor(boolean autoCancel) {
		this.autoCancel = autoCancel;
	}

	@Override
	public BaseProcessor<IN, OUT> start() {
		super.start();
		return this;
	}

	/**
	 * Call {@link #subscribe(Subscriber)} and return the passed {@link Subscriber}, allows for chaining, e.g. :
	 *
	 * {@code Processors.topic().process(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 */
	public <E extends Subscriber<? super OUT>> E process(E s) {
		subscribe(s);
		return s;
	}

	/**
	 * Call {@link #subscribe(Subscriber)} and return the passed {@link Subscriber}, allows for chaining, e.g. :
	 *
	 * {@code Processors.topic().process(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 */
	@SuppressWarnings("unchecked")
	public Publisher<OUT> dispatchOn(ProcessorGroup group) {
		Processor<OUT, OUT> processor = ((ProcessorGroup<OUT>)group).dispatchOn();
		subscribe(processor);
		return processor;
	}

	/**
	 * Call {@link #subscribe(Subscriber)} and return the passed {@link Subscriber}, allows for chaining, e.g. :
	 *
	 * {@code Processors.topic().process(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 */
	@SuppressWarnings("unchecked")
	public Publisher<OUT> publishOn(ProcessorGroup group) {
		Processor<OUT, OUT> processor = ((ProcessorGroup<OUT>)group).publishOn();
		subscribe(processor);
		return processor;
	}

	@Override
	public void onSubscribe(final Subscription s) {
		if (BackpressureUtils.checkSubscription(upstreamSubscription, s)) {
			this.upstreamSubscription = s;
			try {
				doOnSubscribe(s);
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				s.cancel();
				onError(t);
			}
		}
	}

	protected void doOnSubscribe(Subscription s) {
		//IGNORE
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super OUT> s) {
		if (s == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}
	}

	/**
	 * @return a snapshot number of available onNext before starving the resource
	 */
	public long getAvailableCapacity() {
		return getCapacity();
	}

	protected void cancel(Subscription subscription) {
		if (subscription != EmptySubscription.INSTANCE) {
			subscription.cancel();
		}
	}

	@Override
	public Object upstream() {
		return upstreamSubscription;
	}

}
