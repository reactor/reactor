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
package reactor.core.composable.action;

import com.gs.collections.api.block.function.Function2;
import com.gs.collections.api.block.predicate.Predicate;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import org.reactivestreams.api.Producer;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.spi.Subscriber;
import org.reactivestreams.spi.Subscription;
import reactor.alloc.Recyclable;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ActionProcessor<T> implements
		org.reactivestreams.spi.Subscriber<T>,
		org.reactivestreams.spi.Publisher<T>,
		org.reactivestreams.api.Consumer<T>,
		Flushable<T>,
		Recyclable {

	private static enum State {
		READY,
		ERROR,
		COMPLETE,
		SHUTDOWN;
	}

	private final Queue<T>                                   buffer                = new ConcurrentLinkedQueue<T>();
	private final MultiReaderFastList<ActionSubscription<T>> subscriptions         = MultiReaderFastList.newList(8);
	private final MultiReaderFastList<Subscription>          upstreamSubscriptions = MultiReaderFastList.newList(1);
	private final AtomicInteger                              current               = new AtomicInteger(0);

	private long    bufferSize;
	private boolean keepAlive;
	private State     state = State.READY;
	private Throwable error = null;

	public ActionProcessor(long bufferSize, boolean keepAlive) {
		this.bufferSize = bufferSize < 0l ? Long.MAX_VALUE : bufferSize;
		this.keepAlive = keepAlive;
	}

	public ActionProcessor(long bufferSize) {
		this(bufferSize, false);
	}

	@Override
	public Subscriber<T> getSubscriber() {
		return this;
	}

	@Override
	public void subscribe(final Subscriber<T> subscriber) {
		try {
			if (checkState()) {
				final ActionSubscription<T> subscription = new ActionSubscription<T>(this, subscriber);

				if (subscriptions.indexOf(subscription) != -1) {
					subscriber.onError(new IllegalArgumentException("Subscription already exists between this " +
							"publisher/subscriber pair"));
				} else {
					subscriber.onSubscribe(subscription);
					subscriptions.withWriteLockAndDelegate(new ActionMutableListCheckedProcedure<T>(subscription));
				}
			} else {
				if (state == State.COMPLETE) {
					subscriber.onComplete();
				} else if (state == State.SHUTDOWN) {
					subscriber.onError(new IllegalArgumentException("Publisher has shutdown"));
				} else {
					subscriber.onError(error);
				}
			}
		} catch (Throwable cause) {
			subscriber.onError(cause);
		}

	}

	@Override
	public void onSubscribe(final Subscription subscription) {
		if (upstreamSubscriptions.indexOf(subscription) != -1)
			throw new IllegalArgumentException("This Processor is already subscribed");

		upstreamSubscriptions.withWriteLockAndDelegate(new MutableListCheckedProcedure(subscription));
	}

	@Override
	public Flushable<T> flush() {
		if (!checkState()) return this;

		subscriptions.select(new Predicate<ActionSubscription<T>>() {
			@Override
			public boolean accept(ActionSubscription<T> each) {
				return Flushable.class.isAssignableFrom(each.subscriber.getClass());
			}
		}).forEach(new CheckedProcedure<ActionSubscription<?>>() {
			@Override
			public void safeValue(final ActionSubscription<?> object) throws Exception {
				try {
					((Flushable) object.subscriber).flush();
				} catch (final Throwable throwable) {
					subscriptions.select(new Predicate<ActionSubscription<T>>() {
						@Override
						public boolean accept(ActionSubscription<T> each) {
							return each.subscriber == object;
						}
					}).forEach(new CheckedProcedure<ActionSubscription<T>>() {
						@Override
						public void safeValue(ActionSubscription<T> subscription) throws Exception {
							callError(subscription, throwable);
						}
					});
					onError(throwable);
				}
			}
		});

		return this;
	}

	@Override
	public void onNext(final T ev) {
		try {
			if (!checkState() || bufferEvent(ev)) return;

			if (subscriptions.isEmpty()) {
				buffer.add(ev);
				return;
			}

			subscriptions.select(new Predicate<ActionSubscription<T>>() {
				@Override
				public boolean accept(ActionSubscription<T> subscription) {
					return !subscription.cancelled && !subscription.completed && !subscription.error;
				}
			}).forEach(new CheckedProcedure<ActionSubscription<T>>() {
				@Override
				public void safeValue(ActionSubscription<T> subscription) throws Exception {
					try {
						subscription.subscriber.onNext(ev);
					} catch (Throwable throwable) {
						callError(subscription, throwable);
					}
				}
			});

		} catch (final Throwable unrecoverable) {
			state = State.ERROR;
			error = unrecoverable;
			subscriptions.forEach(new CheckedProcedure<ActionSubscription<T>>() {
				@Override
				public void safeValue(ActionSubscription<T> subscription) throws Exception {
					callError(subscription, unrecoverable);
				}
			});
		}

	}

	@Override
	public void onError(final Throwable throwable) {
		if (!checkState()) return;

		if (subscriptions.isEmpty()) return;
		subscriptions.forEach(new CheckedProcedure<ActionSubscription<T>>() {
			@Override
			public void safeValue(ActionSubscription<T> subscription) throws Exception {
				callError(subscription, throwable);
			}
		});
	}

	@Override
	public void onComplete() {
		if (!checkState()) return;

		state = State.COMPLETE;
		if (subscriptions.isEmpty()) return;

		subscriptions.select(new Predicate<ActionSubscription<T>>() {
			@Override
			public boolean accept(ActionSubscription<T> subscription) {
				return !subscription.completed;
			}
		}).forEach(new CheckedProcedure<ActionSubscription<T>>() {
			@Override
			public void safeValue(ActionSubscription<T> subscription) throws Exception {
				try {
					subscription.subscriber.onComplete();
					subscription.completed = true;
				} catch (Throwable throwable) {
					callError(subscription, throwable);
				}
			}
		});
	}

	@Override
	public void recycle() {
		buffer.clear();
		subscriptions.clear();
		upstreamSubscriptions.clear();
		current.set(0);

		state = State.READY;
		bufferSize = 0;
		keepAlive = false;
		error = null;
	}

	public void source(final Producer<T> source) {
		source(source.getPublisher());
	}

	public void source(final Publisher<T> source) {
		source.subscribe(this);
	}

	public void publisherError(Throwable error) {
		this.error = error;
		this.state = State.ERROR;
	}

	public MutableList<ActionSubscription<T>> getSubscriptions() {
		return subscriptions;
	}

	public void setBufferSize(long bufferSize) {
		this.bufferSize = bufferSize;
	}

	public void setKeepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public State getState() {
		return state;
	}

	public Throwable getError() {
		return error;
	}

	private void unsubscribe(final ActionSubscription<T> subscription) {
		subscriptions.withWriteLockAndDelegate(new ActionMutableListCheckedProcedure<T>(subscription) {
			@Override
			public void safeValue(MutableList<ActionSubscription<T>> list) throws Exception {
				list.remove(subscription);

				if (list.isEmpty() && !keepAlive) {
					state = State.SHUTDOWN;
					upstreamSubscriptions.forEach(new CheckedProcedure<Subscription>() {
						@Override
						public void safeValue(Subscription object) throws Exception {
							object.cancel();
						}
					});
				}
			}
		});
	}

	private boolean bufferEvent(T next) {
		if (current.get() > 0) {
			return false;
		}

		buffer.add(next);
		return true;
	}

	private void callError(ActionSubscription<T> subscription, Throwable cause) {
		if (!subscription.error) {
			try{
				subscription.subscriber.onError(cause);
			}catch (ActionException e){
			}
			subscription.error = true;
		}
	}

	private boolean checkState() {
		return state != State.ERROR && state != State.COMPLETE && state != State.SHUTDOWN;

	}

	private void drain(int elements) {

		int min = subscriptions.injectInto(elements, new Function2<Integer, ActionSubscription<T>, Integer>() {
			@Override
			public Integer value(Integer argument1, ActionSubscription<T> argument2) {
				return Math.min(argument2.lastRequested, argument1);
			}
		});

		current.getAndSet(min);

		if (buffer.isEmpty()) return;

		T data;
		int i = 0;
		while ((data = buffer.poll()) != null && i < min) {
			onNext(data);
			i++;
		}
	}

	static class ActionSubscription<T> implements org.reactivestreams.spi.Subscription {
		final Subscriber<T>      subscriber;
		final ActionProcessor<T> publisher;

		int lastRequested;
		boolean cancelled = false;
		boolean completed = false;
		boolean error     = false;

		public ActionSubscription(ActionProcessor<T> publisher, Subscriber<T> subscriber) {
			this.subscriber = subscriber;
			this.publisher = publisher;
		}

		@Override
		public void requestMore(int elements) {
			if (cancelled) {
				return;
			}

			if (elements <= 0) {
				throw new IllegalStateException("Cannot request negative number");
			}

			this.lastRequested = elements;

			publisher.drain(elements);
		}

		@Override
		public void cancel() {
			if (cancelled)
				return;

			cancelled = true;
			publisher.unsubscribe(this);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ActionSubscription that = (ActionSubscription) o;

			if (publisher.hashCode() != that.publisher.hashCode()) return false;
			if (!subscriber.equals(that.subscriber)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = subscriber.hashCode();
			result = 31 * result + publisher.hashCode();
			return result;
		}


		@Override
		public String toString() {
			return "" +
					(cancelled ? "cancelled" : (completed ? "completed" : (error ? "error" : lastRequested)))
					;
		}
	}

	//Various registration procedures

	private static class MutableListCheckedProcedure extends CheckedProcedure<MutableList<Subscription>> {
		private final Subscription subscription;

		public MutableListCheckedProcedure(Subscription subscription) {
			this.subscription = subscription;
		}

		@Override
		public void safeValue(MutableList<Subscription> object) throws Exception {
			object.add(subscription);
		}
	}

	private static class ActionMutableListCheckedProcedure<T> extends CheckedProcedure<MutableList<ActionSubscription<T>>> {
		private final ActionSubscription<T> subscription;

		public ActionMutableListCheckedProcedure(ActionSubscription<T> subscription) {
			this.subscription = subscription;
		}

		@Override
		public void safeValue(MutableList<ActionSubscription<T>> object) throws Exception {
			object.add(subscription);
		}
	}

	private static class FlushableMutableListCheckedProcedure extends
			CheckedProcedure<MutableList<Flushable<?>>> {
		private final Flushable<?> subscription;

		public FlushableMutableListCheckedProcedure(Flushable<?> subscription) {
			this.subscription = subscription;
		}

		@Override
		public void safeValue(MutableList<Flushable<?>> object) throws Exception {
			object.add(subscription);
		}
	}
}