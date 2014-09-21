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
package reactor.rx.action;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.function.Consumer;
import reactor.rx.StreamSubscription;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanInSubscription<O> extends StreamSubscription<O> {


	final List<InnerSubscription> subscriptions;

	protected final ReadWriteLock lock = new ReentrantReadWriteLock();

	public FanInSubscription(Subscriber<O> subscriber,
	                         List<InnerSubscription> subs) {
		super(null, subscriber);
		this.subscriptions = subs;
	}

	@Override
	public void request(final long elements) {
		super.request(elements);
		parallelRequest(elements);
	}

	protected void parallelRequest(long elements) {
		lock.readLock().lock();
		try {
			final int parallel = subscriptions.size();

			if (parallel > 0) {
				final long batchSize = elements / parallel;
				final long remaining = (elements % parallel > 0 ? elements : 0) + batchSize;
				if (batchSize == 0 && elements == 0) return;

				Iterator<InnerSubscription> subscriptionIterator = subscriptions.iterator();
				InnerSubscription subscription;
				while (subscriptionIterator.hasNext()) {
					subscription = subscriptionIterator.next();
					subscription.request(remaining);
					lock.readLock().unlock();
					try {
						pruneObsoleteSub(subscriptionIterator, subscription.toRemove);
					} finally {
						lock.readLock().lock();
					}
				}
			}
		} finally {
			lock.readLock().unlock();
		}
	}

	public void forEach(Consumer<InnerSubscription> consumer) {
		lock.readLock().lock();
		try {
			for (InnerSubscription innerSubscription : subscriptions) {
				consumer.accept(innerSubscription);
			}
		} finally {
			lock.readLock().unlock();
		}
	}

	protected void pruneObsoleteSub(Iterator<InnerSubscription> subscriptionIterator, boolean toRemove) {
		if (toRemove) {
			lock.writeLock().lock();
			try {
				subscriptionIterator.remove();
			} finally {
				lock.writeLock().unlock();
			}
		}
	}

	@Override
	public void cancel() {
		lock.writeLock().lock();
		try {
			for (Subscription subscription : subscriptions) {
				subscription.cancel();
			}
			subscriptions.clear();
		} finally {
			lock.writeLock().unlock();
		}
		subscriptions.clear();
		super.cancel();
	}

	void removeSubscription(final InnerSubscription s) {
		lock.writeLock().lock();
		try {
			subscriptions.remove(s);
		} finally {
			lock.writeLock().unlock();
		}
	}

	void addSubscription(final InnerSubscription s) {
		lock.writeLock().lock();
		try {
			Iterator<InnerSubscription> subscriptionIterator = subscriptions.iterator();
			while (subscriptionIterator.hasNext()) {
				pruneObsoleteSub(subscriptionIterator, subscriptionIterator.next().toRemove);
			}
			subscriptions.add(s);
		} finally {
			lock.writeLock().unlock();
		}
	}
	public static class InnerSubscription implements Subscription {

		final Subscription wrapped;
		boolean toRemove = false;

		public InnerSubscription(Subscription wrapped) {
			this.wrapped = wrapped;
		}

		@Override
		public void request(long n) {
			wrapped.request(n);
		}

		@Override
		public void cancel() {
			wrapped.cancel();
		}

		public Subscription getDelegate() {
			return wrapped;
		}
	}

}
