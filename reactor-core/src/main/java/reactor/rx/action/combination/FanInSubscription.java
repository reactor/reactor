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
package reactor.rx.action.combination;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.queue.internal.MpscLinkedQueue;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.action.support.SerializedSubscriber;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanInSubscription<O, E, X, SUBSCRIBER extends FanInAction.InnerSubscriber<O, E, X>> extends
		ReactiveSubscription<E> implements Subscriber<E> {

	private static final int GC_SUBSCRIPTIONS_TRESHOLD = 16;
	volatile             int runningComposables        = 0;

	static final AtomicIntegerFieldUpdater<FanInSubscription> RUNNING_COMPOSABLE_UPDATER = AtomicIntegerFieldUpdater
			.newUpdater(FanInSubscription.class, "runningComposables");

	protected volatile boolean                                    terminated    = false;
	protected final    Queue<InnerSubscription<O, E, SUBSCRIBER>> subscriptions = MpscLinkedQueue.create();
	protected final    SerializedSubscriber<E>                    serializer    = SerializedSubscriber.create(this);

	public FanInSubscription(Subscriber<? super E> subscriber) {
		super(null, subscriber);
		serializer.onSubscribe(this);
	}

	@Override
	protected void onRequest(final long elements) {
		parallelRequest(elements);
	}

	protected void parallelRequest(long elements) {
		try {
			Action.checkRequest(elements);
			int size = runningComposables;

			if (size > 0) {

				//deal with recursive cancel while requesting
				InnerSubscription<O, E, SUBSCRIBER> subscription;
				int i = 0;
				do {

					//
					synchronized (subscriptions) {
						if (!subscriptions.isEmpty()) {
							subscription = subscriptions.poll();
						} else {
							subscription = null;
						}
					}
					if (subscription != null) {
						if (!subscription.toRemove) {
							subscriptions.add(subscription);
							i++;
						}

						subscription.subscriber.request(elements / size > 0 ? elements / size : elements);

						if (terminated) {
							break;
						}
					}
				} while (i < size && subscription != null);
			} else {
				updatePendingRequests(elements);
			}

			if (terminated) {
				cancel();
			}
		} catch (Throwable t) {
			subscriber.onError(t);
		}
	}

	public void scheduleTermination() {
		terminated = true;
	}

	public void forEach(Consumer<InnerSubscription<O, E, SUBSCRIBER>> consumer) {
		try {
			for (InnerSubscription<O, E, SUBSCRIBER> innerSubscription : subscriptions) {
				consumer.accept(innerSubscription);
			}
		} catch (Throwable t) {
			subscriber.onError(t);
		}
	}

	boolean isDraining(){
		synchronized (this) {
			return draining;
		}
	}

	@Override
	public void cancel() {
		if (!subscriptions.isEmpty()) {
			Subscription s;
			while ((s = subscriptions.poll()) != null) {
				s.cancel();
			}
		}
		super.cancel();
	}

	@SuppressWarnings("unchecked")
	int removeSubscription(final InnerSubscription s) {
		int newSize = RUNNING_COMPOSABLE_UPDATER.decrementAndGet(this);
		if (subscriptions.peek() == s) {
			InnerSubscription removed;
			if ((removed = subscriptions.poll()) != s) {
				if(removed != null) {
					subscriptions.add(removed);
				}
				s.toRemove = true;
			}
		} else {
			s.toRemove = true;
		}
		return newSize;
	}

	@SuppressWarnings("unchecked")
	int addSubscription(final InnerSubscription s) {
		if (terminated) return 0;
		int newSize = RUNNING_COMPOSABLE_UPDATER.incrementAndGet(this);
		int realSize = subscriptions.size();
		if (realSize > GC_SUBSCRIPTIONS_TRESHOLD) {
			InnerSubscription cleaning;
			int i = 0;
			while (i < realSize && (cleaning = subscriptions.poll()) != null) {
				if (!cleaning.toRemove) {
					subscriptions.add(cleaning);
				}
				i++;
			}
		}
		subscriptions.add(s);
		return newSize;
	}

	@Override
	public void updatePendingRequests(long n) {
		super.updatePendingRequests(n);
		if (!subscriptions.isEmpty()) {
			for (InnerSubscription<O, E, SUBSCRIBER> subscription : subscriptions) {
				if(subscription != null && subscription.subscriber != null){
					subscription.subscriber.pendingRequests += n;
				}
			}
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		//IGNORE
	}

	public void serialNext(E next) {
		serializer.onNext(next);
	}

	public void serialError(Throwable t) {
		serializer.onError(t);
	}

	public void serialComplete() {
		serializer.onComplete();
	}

	public void serialRequest(long n) {
		serializer.request(n);
	}

	public void serialCancel() {
		serializer.cancel();
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
