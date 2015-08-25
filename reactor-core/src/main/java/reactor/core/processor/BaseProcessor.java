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
package reactor.core.processor;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscription;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.Bounded;
import reactor.core.support.Resource;
import reactor.fn.Consumer;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A base processor with an async boundary trait to manage active subscribers (Threads), upstream subscription
 * and shutdown options.
 *
 * @author Stephane Maldini
 */
public abstract class BaseProcessor<IN, OUT> extends BaseSubscriber<IN> implements
  Processor<IN, OUT>, Consumer<IN>, Bounded, Resource {

	//protected static final int DEFAULT_BUFFER_SIZE = 1024;

	public static final int SMALL_BUFFER_SIZE  = 32;
	public static final int MEDIUM_BUFFER_SIZE = 8192;

	protected final boolean autoCancel;

	@SuppressWarnings("unused")
	private volatile       int                                      subscriberCount  = 0;
	protected static final AtomicIntegerFieldUpdater<BaseProcessor> SUBSCRIBER_COUNT =
	  AtomicIntegerFieldUpdater
		.newUpdater(BaseProcessor.class, "subscriberCount");

	protected Subscription upstreamSubscription;

	protected BaseProcessor(boolean autoCancel) {
		this.autoCancel = autoCancel;
	}

	@Override
	public BaseProcessor<IN, OUT> start() {
		super.start();
		return this;
	}

	@Override
	public final void accept(IN e) {
		onNext(e);
	}

	/**
	 * @return a snapshot number of available onNext before starving the resource
	 */
	public abstract long getAvailableCapacity();

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	@Override
	public void onSubscribe(final Subscription s) {
		super.onSubscribe(s);
		if (this.upstreamSubscription != null) {
			s.cancel();
			return;
		}
		this.upstreamSubscription = s;
	}

	@Override
	public boolean isExposedToOverflow(Bounded parentPublisher) {
		return false;
	}

	protected boolean incrementSubscribers() {
		return SUBSCRIBER_COUNT.getAndIncrement(this) == 0;
	}

	protected int decrementSubscribers() {
		Subscription subscription = upstreamSubscription;
		int subs = SUBSCRIBER_COUNT.decrementAndGet(this);
		if (subs == 0) {
			if (subscription != null && autoCancel) {
				upstreamSubscription = null;
				subscription.cancel();
			}
			return subs;
		}
		return subs;
	}


}
