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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.*;
import reactor.core.publisher.PublisherFactory;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public class BlockingQueueSubscriber<IN> extends BaseSubscriber<IN> implements ReactiveState.Upstream,
                                                                               ReactiveState.Downstream,
                                                                               Subscription,
                                                                               BlockingQueue<IN> {

	private final Publisher<IN>  source;
	private final Subscriber<IN> target;
	private final Queue<IN>      store;
	private final int            capacity;
	private final boolean        cancelAfterFirstRequestComplete;

	private volatile Throwable endError;
	private volatile boolean   terminated;

	private volatile Subscription subscription;

	private volatile long remainingCapacity;
	private static final AtomicLongFieldUpdater<BlockingQueueSubscriber> REMAINING = AtomicLongFieldUpdater
	  .newUpdater(BlockingQueueSubscriber.class, "remainingCapacity");

	public BlockingQueueSubscriber(Publisher<IN> source, Subscriber<IN> target, Queue<IN> store, int capacity) {
		this(source, target, store, false, capacity);
	}

	public BlockingQueueSubscriber(Publisher<IN> source, Subscriber<IN> target, Queue<IN> store,
	                               boolean cancelAfterFirstRequestComplete, int capacity) {
		Assert.isTrue(store != null, "A queue must be provided");
		Assert.isTrue(capacity > 0, "A strict positive capacity is required");
		this.source = source;
		this.target = target;
		this.cancelAfterFirstRequestComplete = cancelAfterFirstRequestComplete;
		this.capacity = capacity;
		this.store = store;
		if (source != null) {
			source.subscribe(this);
		}
	}

	@Override
	public void request(long n) {
		if (BackpressureUtils.checkRequest(n, target)) {

			if (target != null) {
				long toRequest = n;
				IN polled;
				while ((n == Long.MAX_VALUE || toRequest-- > 0) && (polled = store.poll()) != null) {
					target.onNext(polled);
				}
				Subscription subscription = this.subscription;
				if (toRequest > 0 && subscription != null) {
					subscription.request(toRequest);
				}
			} else {
				BackpressureUtils.getAndAdd(REMAINING, this, n);
				Subscription subscription = this.subscription;
				if (subscription != null) {
					subscription.request(n);
				}
			}

		}
	}

	private boolean terminate() {
		boolean cancel = false;
		if (!terminated) {
			synchronized (this) {
				if (!terminated) {
					cancel = true;
					terminated = true;
				}
			}
		}
		return cancel;
	}

	@Override
	public void cancel() {
		Subscription subscription = this.subscription;

		if (terminate() && subscription != null) {
			subscription.cancel();
			this.subscription = null;
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (BackpressureUtils.checkSubscription(subscription, s)) {
			this.subscription = s;
			if (source == null && target != null) {
				target.onSubscribe(this);
			} else {
				request(capacity == Integer.MAX_VALUE ? Long.MAX_VALUE : capacity);
			}
		}
	}

	@Override
	public void onNext(IN in) {
		super.onNext(in);
		if (terminated) throw CancelException.get();

		long r = REMAINING.decrementAndGet(this);

		if (source == null && target != null) {
			target.onNext(in);

		} else if (!store.offer(in)) {
			onError(InsufficientCapacityException.get());
		}

		if (cancelAfterFirstRequestComplete && r == 0) {
			cancel();
		}
	}

	@Override
	public Subscriber<? super IN> downstream() {
		return target;
	}

	@Override
	public Object upstream() {
		return subscription;
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);
		if (terminate()) {
			subscription = null;
			endError = t;
			if (source == null && target != null) {
				target.onError(t);
			}
		}
	}

	@Override
	public void onComplete() {
		if (terminate()) {
			subscription = null;
			if (source == null && target != null) {
				target.onComplete();
			}
		}
	}

	@Override
	public boolean add(IN in) {
		if (target == null) {
			throw new UnsupportedOperationException("This operation requires a write queue");
		}
		if (remainingCapacity == 0) throw new IllegalStateException("no space");
		onNext(in);
		return true;
	}

	@Override
	public boolean offer(IN in) {
		if (target == null) {
			throw new UnsupportedOperationException("This operation requires a write queue");
		}
		if (remainingCapacity == 0) return false;
		onNext(in);
		return false;
	}

	@Override
	public void put(IN in) throws InterruptedException {
		if (target == null) {
			throw new UnsupportedOperationException("This operation requires a write queue");
		}
		onNext(in);
	}

	@Override
	public boolean offer(IN in, long timeout, TimeUnit unit) throws InterruptedException {
		if (target == null) {
			throw new UnsupportedOperationException("This operation requires a write queue");
		}
		long timespan = System.currentTimeMillis() +
		  TimeUnit.MILLISECONDS.convert(timeout, unit);

		while (!store.offer(in)) {
			if (blockingTerminatedCheck() || System.currentTimeMillis() > timespan) {
				return false;
			}
			Thread.sleep(10);
		}
		return true;
	}

	private boolean blockingTerminatedCheck() {
		if (terminated) {
			if (endError != null) {
				Exceptions.throwIfFatal(endError);
				throw ReactorFatalException.create(endError);
			}
			return true;
		}
		return false;
	}

	@Override
	@SuppressWarnings("unchecked")
	public IN take() throws InterruptedException {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}

		markRead();

		IN res;
		while ((res = store.poll()) == null) {
			if (blockingTerminatedCheck()) break;
			if (remainingCapacity == 0){
				markAllRead();
			}
			Thread.sleep(10);
		}
		return res;
	}

	@Override
	public IN poll(long timeout, TimeUnit unit) throws InterruptedException {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}
		IN res;

		markRead();
		long timespan = System.currentTimeMillis() +
		  TimeUnit.MILLISECONDS.convert(timeout, unit);

		while ((res = store.poll()) == null) {
			if (blockingTerminatedCheck()) throw CancelException.get();
			if (remainingCapacity == 0);
			if (System.currentTimeMillis() > timespan) {
				break;
			}
			Thread.sleep(10);
		}
		return res;
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean remove(Object o) {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}
		if (store.remove(o)) {
			markRead();
			return true;
		}
		return false;
	}

	@Override
	public int drainTo(Collection<? super IN> c) {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}
		if (store instanceof BlockingQueue) {
			int drained = ((BlockingQueue<IN>) store).drainTo(c);
			if (drained > 0) {
				markAllRead();
				return drained;
			}
		}
		return 0;
	}

	@Override
	public int drainTo(Collection<? super IN> c, int maxElements) {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}
		if (store instanceof BlockingQueue) {
			int drained = ((BlockingQueue<IN>) store).drainTo(c, maxElements);
			if (drained > 0) {
				markAllRead();
				return drained;
			}
		}
		return 0;
	}

	@Override
	public IN remove() {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}
		IN t = store.remove();
		if (t == null) {
			throw new NoSuchElementException();
		} else {
			markRead();
			return t;
		}
	}

	@Override
	public IN poll() {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}


		markRead();
		IN res = store.poll();
		if(res == null && blockingTerminatedCheck()) throw CancelException.get();
		return res;
	}

	private void markRead() {
		long r = remainingCapacity;
		if (r == 0L && target == null) {
			long toRequest = capacity - store.size();
			if(BackpressureUtils.getAndAdd(REMAINING, this, toRequest) == 0) {
				Subscription subscription = this.subscription;
				if (toRequest > 0 && subscription != null) {
					subscription.request(toRequest);
				}
			}
		}
	}

	private void markAllRead() {
		request(capacity - store.size());
	}

	@Override
	public boolean addAll(Collection<? extends IN> c) {
		if (target == null) {
			throw new UnsupportedOperationException("This operation requires a write queue");
		}

		if (!c.isEmpty()) {
			for (IN in : c) {
				offer(in);
			}
			return true;
		}

		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}
		if (store.removeAll(c)) {
			markAllRead();
			return true;
		}
		return false;
	}


	@Override
	public boolean retainAll(Collection<?> c) {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}
		if (store.retainAll(c)) {
			markAllRead();
			return true;
		}
		return false;
	}

	@Override
	public void clear() {
		if (source == null) {
			throw new UnsupportedOperationException("This operation requires a read queue");
		}
		store.clear();
		markAllRead();
	}

	@Override
	public int remainingCapacity() {
		return (int) remainingCapacity;
	}

	@Override
	public boolean contains(Object o) {
		return store.contains(o);
	}

	@Override
	public IN element() {
		return store.element();
	}

	@Override
	public IN peek() {
		return store.peek();
	}

	@Override
	public int size() {
		return store.size();
	}

	@Override
	public boolean isEmpty() {
		return store.isEmpty();
	}

	@Override
	public Iterator<IN> iterator() {
		return store.iterator();
	}

	@Override
	public Object[] toArray() {
		return store.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return store.toArray(a);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return store.containsAll(c);
	}

}
