/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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
package reactor.rx.subscription;

import reactor.function.Consumer;
import reactor.rx.Stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A composite subscription used to achieve pub/sub pattern. When more than 1 subscriber is attached to a Stream,
 * in particular an Action, the previous subscription is replaced by a composite fanOutSubscription delegating to
 * both the previous and the new subscriptions.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanOutSubscription<O> extends PushSubscription<O> {

	private final List<PushSubscription<O>> subscriptions = new ArrayList<PushSubscription<O>>(2);
	private final ReentrantReadWriteLock    lock          = new ReentrantReadWriteLock();

	public FanOutSubscription(Stream<O> publisher, PushSubscription<O> reactiveSubscriptionA,
	                          PushSubscription<O> reactiveSubscriptionB) {
		super(publisher, null);
		subscriptions.add(reactiveSubscriptionA);
		subscriptions.add(reactiveSubscriptionB);
	}

	@Override
	public void onComplete() {
		forEach(new Consumer<PushSubscription<O>>() {
			@Override
			public void accept(PushSubscription<O> subscription) {
				try {
					subscription.onComplete();
				} catch (Throwable throwable) {
					subscription.onError(throwable);
				}
			}
		});
	}

	@Override
	public void onNext(final O ev) {

		forEach(new Consumer<PushSubscription<O>>() {
			@Override
			public void accept(PushSubscription<O> subscription) {
				try {
					subscription.onNext(ev);

				} catch (Throwable throwable) {
					subscription.onError(throwable);
				}
			}
		});
	}

	@Override
	public void cancel() {
		forEach(new Consumer<PushSubscription<O>>() {
			@Override
			public void accept(PushSubscription<O> oPushSubscription) {
				oPushSubscription.cancel();
			}
		});
		super.cancel();
	}

	@Override
	public void onError(final Throwable ev) {
		forEach(new Consumer<PushSubscription<O>>() {
			@Override
			public void accept(PushSubscription<O> oPushSubscription) {
				oPushSubscription.onError(ev);
			}
		});
	}

	@Override
	public boolean isComplete() {
		lock.readLock().lock();
		try {
			boolean isComplete = false;
			for (PushSubscription<O> subscription : subscriptions) {
				isComplete = subscription.isComplete();
				if (!isComplete) break;
			}
			return isComplete;
		} finally {
			lock.readLock().unlock();
		}
	}

	public void forEach(Consumer<PushSubscription<O>> consumer) {
		lock.readLock().lock();
		try {
			for (PushSubscription<O> subscription : subscriptions) {
				if (subscription != null) {
					consumer.accept(subscription);
				}
			}
		} finally {
			lock.readLock().unlock();
		}
	}

	public List<PushSubscription<O>> getSubscriptions() {
		return Collections.unmodifiableList(subscriptions);
	}

	public boolean isEmpty() {
		lock.readLock().lock();
		try {
			return subscriptions.isEmpty();
		} finally {
			lock.readLock().unlock();
		}
	}

	public boolean remove(PushSubscription<O> subscription) {
		lock.writeLock().lock();
		try {
			return subscriptions.remove(subscription);
		} finally {
			lock.writeLock().unlock();
		}
	}


	public boolean add(PushSubscription<O> subscription) {
		lock.writeLock().lock();
		try {
			return subscriptions.add(subscription);
		} finally {
			lock.writeLock().unlock();
		}
	}

	public boolean contains(PushSubscription<O> subscription) {
		lock.readLock().lock();
		try {
			return subscriptions.contains(subscription);
		} finally {
			lock.readLock().unlock();
		}
	}
}
