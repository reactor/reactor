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
package reactor.rx.subscription;

import org.reactivestreams.Subscriber;
import reactor.core.queue.CompletableLinkedQueue;
import reactor.core.queue.CompletableQueue;
import reactor.rx.Stream;
import reactor.rx.action.Action;

/**
 * Relationship between a Stream (Publisher) and a Subscriber.
 * <p>
 * A Reactive Subscription using a pattern called "reactive-pull" to dynamically adapt to the downstream subscriber
 * capacity:
 * - If no capacity (no previous request or capacity drained), queue data into the buffer {@link CompletableQueue}
 * - If capacity (previous request and capacity remaining), call subscriber onNext
 * <p>
 * Queued data will be polled when the next request(n) signal is received. If there is remaining requested volume,
 * it will be added to the current capacity and therefore will let the next signals to be directly pushed.
 * Each next signal will decrement the capacity by 1.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class ReactiveSubscription<O> extends PushSubscription<O> {


	protected final CompletableQueue<O> buffer;

	//Guarded by this
	protected boolean draining = false;

	//Only read from subscriber context
	protected volatile long currentNextSignals = 0l;

	//Can be set outside of publisher and subscriber contexts
	protected volatile long maxCapacity = Long.MAX_VALUE;

	public ReactiveSubscription(Stream<O> publisher, Subscriber<? super O> subscriber) {
		this(publisher, subscriber, new CompletableLinkedQueue<O>());
	}

	public ReactiveSubscription(Stream<O> publisher, Subscriber<? super O> subscriber, CompletableQueue<O> buffer) {
		super(publisher, subscriber);
		this.buffer = buffer;
	}

	@Override
	public void request(long elements) {
		try {
			Action.checkRequest(elements);

			O element;
			FastList list = null;
			long toRequest = elements;// Math.min(maxCapacity, elements);
			boolean last;

			do {
				synchronized (this) {
					//Subscription terminated, Buffer done, return immediately
					if (terminated == 1) {
						return;
					}

					//If unbounded request, set and return
					if (toRequest == Long.MAX_VALUE) {
						//System.out.println(subscriber.getClass().getSimpleName()+" el:"+elements);
						if (pendingRequestSignals == Long.MAX_VALUE) {
							return;
						} else {
							pendingRequestSignals = Long.MAX_VALUE;
						}
					} else {
						long previous = pendingRequestSignals;
						if (previous != Long.MAX_VALUE && PENDING_UPDATER.addAndGet(this, toRequest) < 0l) {
							PENDING_UPDATER.set(this, Long.MAX_VALUE);
							//onError(SpecificationExceptions.spec_3_17_exception(publisher, subscriber, previous,
							// toRequest));
							return;
						}
					}

					draining = !buffer.isEmpty();

					if (draining) {
						list = new FastList();
						while (list.size < toRequest && (element = buffer.poll()) != null) {
							list.add(element);
						}

						if (list.size != 0 && pendingRequestSignals != Long.MAX_VALUE) {
							if (PENDING_UPDATER.addAndGet(this, -list.size) < 0) {
								pendingRequestSignals = 0l;
							}
						}
					} else {
						currentNextSignals = 0;
					}
				}

				if (list != null) {
					drainNext(list);
				} else {
					//started
					if (terminated == 0) {
						onRequest(elements);
					}
					return;
				}

				synchronized (this) {
					draining = !buffer.isEmpty();
					last = !draining && buffer.isComplete();
				}

				if (last) {
					onComplete();
				} else {
					if (elements != Long.MAX_VALUE) {
						elements -= list.size;
					}
					if (draining) {
						toRequest = elements;
					} else if (elements > 0l) {
						//started
						if (terminated == 0) {
							onRequest(elements);
						} else {
							updatePendingRequests(elements);
						}
						toRequest = 0;
					}
				}
			} while (draining && toRequest > 0);

		} catch (Exception e) {
			onError(e);
		}

	}

	@SuppressWarnings("unchecked")
	private void drainNext(FastList list) {
		if (list.size > 0) {
			for (Object el : list.array) {
				if (el == null) break;
				currentNextSignals++;
				subscriber.onNext((O) el);
			}
		}
	}

	@Override
	public void onNext(O ev) {

		synchronized (this) {
			/*if (draining) {
				if (ev != null) {
					if (pendingRequestSignals != Long.MAX_VALUE) {
						PENDING_UPDATER.incrementAndGet(this);
					}
					buffer.add(ev);
				}
				return;
			} else */

			if (pendingRequestSignals != Long.MAX_VALUE &&
			  PENDING_UPDATER.decrementAndGet(this) < 0l) {
				PENDING_UPDATER.incrementAndGet(this);
				if (ev != null) {
					buffer.add(ev);
				}
				return;
			} else {
				currentNextSignals++;
			}
		}

		subscriber.onNext(ev);
	}

	@Override
	public void onComplete() {
		boolean complete = false;
		if (terminated == 1)
			return;

		synchronized (this) {
			buffer.complete();

			if (buffer.isEmpty()) {
				if (TERMINAL_UPDATER.compareAndSet(this, 0, 1) && subscriber != null) {
					complete = true;
				}
			}
		}

		if (complete) {
			subscriber.onComplete();
		}
	}

	public long currentNextSignals() {
		return currentNextSignals;
	}

	@Override
	public void updatePendingRequests(long n) {
		long oldPending;
		long newPending;

		synchronized (this) {
			oldPending = pendingRequestSignals;
			newPending = n == 0l ? 0l : oldPending + n;
			if (newPending < 0) {
				newPending = n > 0 ? Long.MAX_VALUE : 0;
			}
			pendingRequestSignals = newPending;
		}
	}

	@Override
	public boolean shouldRequestPendingSignals() {
		synchronized (this) {
			return pendingRequestSignals > 0 && pendingRequestSignals != Long.MAX_VALUE
			  && (!buffer.isEmpty() || currentNextSignals == maxCapacity);
		}
	}

	@Override
	public final void maxCapacity(long maxCapacity) {
		this.maxCapacity = maxCapacity;
	}

	public final long getBufferSize() {
		return buffer != null ? buffer.size() : -1l;
	}

	public final long capacity() {
		return pendingRequestSignals;
	}

	public final CompletableQueue<O> getBuffer() {
		return buffer;
	}

	@Override
	public final boolean isComplete() {
		synchronized (this) {
			return buffer.isEmpty() && buffer.isComplete();
		}
	}

	@Override
	public String toString() {
		return "{" +
		  "current=" + currentNextSignals +
		  ", pending=" + (pendingRequestSignals() == Long.MAX_VALUE ? "infinite" : pendingRequestSignals()) +
		  (buffer != null ? (terminated == 1 ? ", complete" : "") + (", waiting=" + buffer.size()) : "") +
		  '}';
	}

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
}
