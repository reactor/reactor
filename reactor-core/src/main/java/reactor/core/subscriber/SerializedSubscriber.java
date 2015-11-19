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
import reactor.core.publisher.PublisherFactory;
import reactor.core.support.Bounded;
import reactor.core.support.Publishable;
import reactor.core.support.Subscribable;

/**
 * Enforces single-threaded, serialized, ordered execution of {@link #onNext}, {@link #onComplete},
 * {@link #onError}, {@link #request} and {@link #cancel}.
 * <p>
 * When multiple threads are emitting and/or notifying they will be serialized by:
 * </p><ul>
 * <li>Allowing only one thread at a time to emit</li>
 * <li>Adding notifications to a queue if another thread is already emitting</li>
 * <li>Not holding any locks or blocking any threads while emitting</li>
 * </ul>
 *
 * @param <T> the type of items expected to be observed by the {@code Observer}
 *            <p>
 *            Port from RxJava's SerializedObserver applied to Reactive Stream
 */
public class SerializedSubscriber<T> implements Subscriber<T>, Subscription, Bounded, Publishable<T>, Subscribable<T> {
	private final Subscriber<? super T> delegate;

	private boolean emitting   = false;
	private boolean terminated = false;
	private FastList     queue;
	private Subscription subscription;

	private static final int    MAX_DRAIN_ITERATION = Integer.MAX_VALUE;
	private static final Object NULL_SENTINEL       = new Object();
	private static final Object COMPLETE_SENTINEL   = new Object();

	static final class FastList {
		Object[] array;
		int      size;

		public void add(Object o) {
			int s = size;
			Object[] a = array;
			if (a == null) {
				a = new Object[16];
				array = a;
			} else if (s == a.length) {
				Object[] array2 = new Object[s + (s >> 2)];
				System.arraycopy(a, 0, array2, 0, s);
				a = array2;
				array = a;
			}
			a[s] = o;
			size = s + 1;
		}
	}

	@Override
	public Subscriber<? super T> downstream() {
		return delegate;
	}

	@Override
	public Publisher<T> upstream() {
		return PublisherFactory.fromSubscription(subscription);
	}

	private static final class ErrorSentinel {
		final Throwable e;

		ErrorSentinel(Throwable e) {
			this.e = e;
		}
	}

	public static <T> SerializedSubscriber<T> create(Subscriber<? super T> s) {
		return new SerializedSubscriber<>(s);
	}

	private SerializedSubscriber(Subscriber<? super T> s) {
		this.delegate = s;
	}

	@Override
	public void onSubscribe(final Subscription s) {
		this.subscription = s;
		delegate.onSubscribe(this);
	}

	@Override
	public void onComplete() {
		FastList list;
		synchronized (this) {
			if (terminated) {
				return;
			}
			terminated = true;
			if (emitting) {
				if (queue == null) {
					queue = new FastList();
				}
				queue.add(COMPLETE_SENTINEL);
				return;
			}
			emitting = true;
			list = queue;
			queue = null;
		}
		drainQueue(list);
		delegate.onComplete();
	}

	@Override
	public void onError(final Throwable e) {
		//TODO throw if fatal ?;
		FastList list;
		synchronized (this) {
			if (terminated) {
				return;
			}
			if (emitting) {
				if (queue == null) {
					queue = new FastList();
				}
				queue.add(new ErrorSentinel(e));
				return;
			}
			emitting = true;
			list = queue;
			queue = null;
		}
		drainQueue(list);
		delegate.onError(e);
		synchronized (this) {
			emitting = false;
		}
	}

	@Override
	public void onNext(T t) {
		FastList list;

		synchronized (this) {
			if (terminated) {
				return;
			}
			if (emitting) {
				if (queue == null) {
					queue = new FastList();
				}
				queue.add(t != null ? t : NULL_SENTINEL);
				// another thread is emitting so we add to the queue and return
				return;
			}
			// we can emit
			emitting = true;
			// reference to the list to drain before emitting our value
			list = queue;
			queue = null;
		}

		// we only get here if we won the right to emit, otherwise we returned in the if(emitting) block above
		boolean skipFinal = false;
		try {
			int iter = MAX_DRAIN_ITERATION;
			do {
				drainQueue(list);
				if (iter == MAX_DRAIN_ITERATION) {
					// after the first draining we emit our own value
					delegate.onNext(t);
				}
				--iter;
				if (iter > 0) {
					synchronized (this) {
						list = queue;
						queue = null;
						if (list == null) {
							emitting = false;
							skipFinal = true;
							return;
						}
					}
				}
			} while (iter > 0);
		} finally {
			if (!skipFinal) {
				synchronized (this) {
					if (terminated) {
						list = queue;
						queue = null;
					} else {
						emitting = false;
						list = null;
					}
				}
			}
		}

		// this will only drain if terminated (done here outside of synchronized block)
		drainQueue(list);
	}

	@Override
	public void request(long n) {
		if (subscription != null) {
			subscription.request(n);
		}
	}

	@Override
	public void cancel() {
		if (subscription != null) {
			subscription.cancel();
		}
	}

	void drainQueue(FastList list) {
		if (list == null || list.size == 0) {
			return;
		}
		Object v;
		for (int i = 0; i < list.size; i++) {
			v = list.array[i];
			if (v == null) {
				break;
			}
			if (v == NULL_SENTINEL) {
				delegate.onNext(null);
			} else if (v == COMPLETE_SENTINEL) {
				delegate.onComplete();
				return;
			} else if (v.getClass() == ErrorSentinel.class) {
				delegate.onError(((ErrorSentinel) v).e);
			} else {
				@SuppressWarnings("unchecked")
				T t = (T) v;
				delegate.onNext(t);
			}
		}
	}

	@Override
	public String toString() {
		FastList queue;
		synchronized (this) {
			queue = this.queue;
			if (queue == null) return "";
		}
		String res = "{";

		for (Object o : queue.array) {
			res += o + " ";
		}

		return res + "}";
	}

	@Override
	public boolean isExposedToOverflow(Bounded parent) {
		return false;
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}
}
