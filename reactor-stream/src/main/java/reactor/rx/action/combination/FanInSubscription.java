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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SerializedSubscriber;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanInSubscription<O, E, X, SUBSCRIBER extends FanInAction.InnerSubscriber<O, E, X>> extends
		ReactiveSubscription<E> implements Subscriber<E> {

	volatile int runningComposables = 0;

	static final AtomicIntegerFieldUpdater<FanInSubscription> RUNNING_COMPOSABLE_UPDATER = AtomicIntegerFieldUpdater
			.newUpdater(FanInSubscription.class, "runningComposables");

	protected final FastList                subscriptions = new FastList();
	protected final SerializedSubscriber<E> serializer    = SerializedSubscriber.create(this);

	protected volatile boolean terminated = false;
	protected          int     leftIndex  = Integer.MAX_VALUE;

	public FanInSubscription(Subscriber<? super E> subscriber) {
		super(null, subscriber);
		Publishers.trampoline(new Publisher<E>() {
			@Override
			public void subscribe(Subscriber<? super E> s) {
				s.onSubscribe(FanInSubscription.this);
			}

		}).subscribe(serializer);
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

				FanInAction.InnerSubscriber sub;
				int i;
				FanInAction.InnerSubscriber[] subs;
				int arraySize;
				synchronized (this) {
					if (subscriptions.size == 0) {
						return;
					}
					subs = subscriptions.array;
					arraySize = subscriptions.size;
				}

				for (i = 0; i < arraySize; i++) {
					sub = subs[i];
					if (sub != null) {
						sub.request(elements);

						if (terminated) {
							break;
						}
					}
				}
			}


		} catch (Throwable t) {
			subscriber.onError(t);
		}
	}

	@SuppressWarnings("unchecked")
	public void forEach(Consumer<InnerSubscription<O, E, SUBSCRIBER>> consumer) {
		try {
			FanInAction.InnerSubscriber[] subs;
			int size;
			synchronized (this) {
				if (subscriptions.size == 0) {
					return;
				}
				subs = subscriptions.array;
				size = subscriptions.size;
			}

			if (size > 0) {
				FanInAction.InnerSubscriber sub;
				for (int i = 0; i < size; i++) {
					sub = subs[i];
					if (sub != null) {
						consumer.accept(sub.s);
					}
				}
			}
		} catch (Throwable t) {
			subscriber.onError(t);
		}
	}

	@Override
	public void cancel() {
		super.cancel();

		FanInAction.InnerSubscriber[] subs;
		int size;
		synchronized (this) {
			if (subscriptions.size == 0) {
				return;
			}
			subs = subscriptions.array;
			size = subscriptions.size;
		}

		FanInAction.InnerSubscriber sub = null;
		for (int i = 0; i < size; i++) {
			synchronized (this) {
				if (subs[i] != null) {
					sub = subs[i];
					subs[i] = null;
				}
			}
			if (sub != null) {
				sub.cancel();
			}
		}

		synchronized (this) {
			subscriptions.clear();
		}
	}

	@SuppressWarnings("unchecked")
	int addSubscription(final FanInAction.InnerSubscriber s) {
		if (terminated) return 0;
		synchronized (this) {
			if(leftIndex < subscriptions.size && subscriptions.array[leftIndex] == null){
				subscriptions.array[leftIndex] = s;
			}else{
				subscriptions.add(s);
				leftIndex = subscriptions.size - 1;
			}
			return leftIndex;
		}
	}

	void remove(int sequenceId) {
		synchronized (this) {
			if (sequenceId < subscriptions.size) {
				subscriptions.array[sequenceId] = null;
				leftIndex = leftIndex > sequenceId ? sequenceId : leftIndex;
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected InnerSubscription<O, E, SUBSCRIBER> shift(int sequenceId) {
		FanInAction.InnerSubscriber sub;
		synchronized (this) {
			if (sequenceId < subscriptions.size) {
				subscriptions.array[sequenceId] = null;
				for (int i = 0; i < subscriptions.size; i++) {
					sub = subscriptions.array[i];
					if (sub != null) {
						return sub.s;
					}
				}
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	protected InnerSubscription<O, E, SUBSCRIBER> peek() {
		synchronized (this) {
			for (int i = 0; i < subscriptions.size; i++) {
				if (subscriptions.array[i] != null) {
					return subscriptions.array[i].s;
				}
			}
		}
		return null;
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


	public void safeRequest(long n) {
		serializer.request(n);
	}

	@Override
	public String toString() {
		return super.toString() + serializer;
	}

	public static class InnerSubscription<O, E, SUBSCRIBER
			extends FanInAction.InnerSubscriber<O, E, ?>> implements Subscription {

		final SUBSCRIBER   subscriber;

		//lazy mutable to respect ordering for peek() operations
		Subscription wrapped;

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

	static final class FastList {
		FanInAction.InnerSubscriber[] array;
		int                           size;

		public void add(FanInAction.InnerSubscriber o) {
			int s = size;
			FanInAction.InnerSubscriber[] a = array;
			if (a == null) {
				a = new FanInAction.InnerSubscriber[16];
				array = a;
			} else if (s == a.length) {
				FanInAction.InnerSubscriber[] array2 = new FanInAction.InnerSubscriber[s + (s >> 2)];
				System.arraycopy(a, 0, array2, 0, s);
				a = array2;
				array = a;
			}
			a[s] = o;
			size = s + 1;
		}

		public void clear() {
			array = null;
			size = 0;
		}
	}

}
