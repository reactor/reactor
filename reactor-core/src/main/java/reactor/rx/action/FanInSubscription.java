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
package reactor.rx.action;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.function.Consumer;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanInSubscription<O, E, X, SUBSCRIBER extends FanInAction.InnerSubscriber<O, E, X>> extends
		ReactiveSubscription<E> {


	final List<InnerSubscription<O, E, SUBSCRIBER>> subscriptions;

	protected final    ReadWriteLock lock       = new ReentrantReadWriteLock();
	protected volatile boolean       terminated = false;

	public FanInSubscription(Subscriber<? super E> subscriber,
	                         List<InnerSubscription<O, E, SUBSCRIBER>> subs) {
		super(null, subscriber);
		this.subscriptions = subs;
	}

	@Override
	protected void onRequest(final long elements) {
		parallelRequest(elements);
	}

	protected void parallelRequest(long elements) {
		lock.writeLock().lock();
		try {
			int size = subscriptions.size();

			if (size > 0) {
				if (elements == 0) return;

				//deal with recursive cancel while requesting
				int i = 0;
				InnerSubscription<O, E, SUBSCRIBER> subscription;
				while (i < size) {

					subscription = subscriptions.get(i);
					subscription.subscriber.request(elements);
					if (subscription.toRemove) {
						size--;
						if (i > 0) i--;
					} else {
						i++;
					}
					if (terminated) {
						break;
					}
				}
			} else {
				updatePendingRequests(elements);
			}

			if (terminated) {
				cancel();
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	public void scheduleTermination() {
		terminated = true;
	}

	public void forEach(Consumer<InnerSubscription<O, E, SUBSCRIBER>> consumer) {
		lock.readLock().lock();
		try {
			for (InnerSubscription<O, E, SUBSCRIBER> innerSubscription : subscriptions) {
				consumer.accept(innerSubscription);
			}
		} finally {
			lock.readLock().unlock();
		}
	}

	protected void pruneObsoleteSub(Iterator<InnerSubscription<O, E, SUBSCRIBER>> subscriptionIterator,
	                                boolean toRemove) {
		if (toRemove) {
			lock.writeLock().lock();
			try {
				subscriptionIterator.remove();
			} finally {
				lock.writeLock().unlock();
			}
		}
	}


	public List<InnerSubscription<O, E, SUBSCRIBER>> unsafeImmutableSubscriptions() {
		return Collections.unmodifiableList(subscriptions);
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

	@SuppressWarnings("unchecked")
	void removeSubscription(final InnerSubscription s) {
		lock.writeLock().lock();
		try {
			subscriptions.remove(s);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@SuppressWarnings("unchecked")
	void addSubscription(final InnerSubscription s) {
		lock.writeLock().lock();
		try {
			Iterator<InnerSubscription<O, E, SUBSCRIBER>> subscriptionIterator = subscriptions.iterator();
			while (subscriptionIterator.hasNext()) {
				pruneObsoleteSub(subscriptionIterator, subscriptionIterator.next().toRemove);
			}
			subscriptions.add(s);
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long clearPendingRequest() {
		long res = super.clearPendingRequest();
		if(Long.MAX_VALUE == res){
			return res;
		}
		lock.readLock().lock();
		try {
			if (!subscriptions.isEmpty()) {
				for (InnerSubscription<O, E, SUBSCRIBER> subscription : subscriptions) {
					res += subscription.subscriber.pendingRequests;
					subscription.subscriber.pendingRequests = 0;
				}
			}
		} finally {
			lock.readLock().unlock();
		}
		return res;
	}

	@Override
	public boolean hasPublisher() {
		return super.hasPublisher();
	}

	public static class InnerSubscription<O, E, SUBSCRIBER
			extends FanInAction.InnerSubscriber<O, E, ?>> implements Subscription {

		final SUBSCRIBER   subscriber;
		final Subscription wrapped;
		boolean toRemove = false;

		public InnerSubscription(Subscription wrapped, SUBSCRIBER subscriber) {
			this.wrapped = wrapped;
			this.subscriber = subscriber;
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
