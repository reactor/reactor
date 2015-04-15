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
import reactor.core.Dispatcher;
import reactor.core.support.NonBlocking;
import reactor.fn.Consumer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A base processor
 *
 * @author Stephane Maldini
 */
public abstract class ReactorProcessor<E> implements Processor<E, E>, Consumer<E>, NonBlocking {

	protected static final int DEFAULT_BUFFER_SIZE = 1024;
	protected static final int SMALL_BUFFER_SIZE   = 32;
	protected final boolean autoCancel;

	protected final ClassLoader context = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
	};

	protected final AtomicLong subscriberCount;

	protected Subscription upstreamSubscription;

	public ReactorProcessor(boolean autoCancel) {
		this.subscriberCount = new AtomicLong(0);
		this.autoCancel = autoCancel;
	}

	@Override
	public final void accept(E e) {
		onNext(e);
	}

	@Override
	public void onSubscribe(final Subscription s) {
		if (this.upstreamSubscription != null) {
			s.cancel();
			return;
		}
		this.upstreamSubscription = s;
	}

	protected boolean incrementSubscribers() {
		return subscriberCount.getAndIncrement() == 0l;
	}

	protected boolean decrementSubscribers() {
		Subscription subscription = upstreamSubscription;
		if (subscriberCount.decrementAndGet() == 0l) {
			if (subscription != null && autoCancel) {
				upstreamSubscription = null;
				subscription.cancel();
			}
			return true;
		}
		return false;
	}

	public abstract long getAvailableCapacity();

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
		return false;
	}
}
