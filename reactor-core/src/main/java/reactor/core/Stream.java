/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.core;

import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Function;
import reactor.fn.Observable;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.selector.Selector;
import reactor.fn.support.Reduce;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static reactor.fn.Functions.$;

/**
 * A {@literal Stream} may be triggered several times, e.g. by processing a collection,
 * and have dedicated data group processing methods.
 *
 * @param <T> The  {@link Stream} output type.
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class Stream<T> extends Composable<T> {

	private final Object   firstKey      = new Object();
	private final Selector firstSelector = $(firstKey);

	private final Object   lastKey      = new Object();
	private final Selector lastSelector = $(lastKey);

	/**
	 * Create a {@link Stream} that uses the given {@link Reactor} for publishing events internally.
	 *
	 * @param observable The {@link Reactor} to use.
	 */
	Stream(Environment env, Observable observable) {
		super(env, observable);
	}

	/**
	 * Set the number of times to expect {@link #accept(Object)} to be called.
	 *
	 * @param expectedAcceptCount The number of times {@link #accept(Object)} will be called.
	 * @return {@literal this}
	 */
	@Override
	public Stream<T> setExpectedAcceptCount(long expectedAcceptCount) {
		boolean notifyLast = false;
		synchronized (monitor) {
			doSetExpectedAcceptCount(expectedAcceptCount);
			if (acceptCountReached()) {
				monitor.notifyAll();
				notifyLast = true;
			}
		}

		if (notifyLast) {
			getObservable().notify(lastKey, Event.wrap(getValue()));
		}

		return this;
	}

	/**
	 * Creates a new {@link Composable} that will be triggered once, the first time {@link #accept(Object)} is called on
	 * the parent.
	 *
	 * @return A new {@link Composable} that is linked to the parent.
	 */
	public Stream<T> first() {
		final Stream<T> c = (Stream<T>) this.assignComposable(getObservable());
		c.doSetExpectedAcceptCount(1);

		when(firstSelector, new Consumer<T>() {
			@Override
			public void accept(T t) {
				c.accept(t);
			}
		});

		return c;
	}

	/**
	 * Creates a new {@link Stream} that will be triggered once, the last time {@link #accept(Object)} is called on the
	 * parent.
	 *
	 * @return A new {@link Stream} that is linked to the parent.
	 * @see {@link #setExpectedAcceptCount(long)}
	 */
	public Stream<T> last() {
		final Stream<T> c = (Stream<T>) this.assignComposable(getObservable());
		c.doSetExpectedAcceptCount(1);

		when(lastSelector, new Consumer<T>() {
			@Override
			public void accept(T t) {
				c.accept(t);
			}
		});
		return c;
	}

	/**
	 * Accumulate a result until expected accept count has been reached - If this limit hasn't been set, each accumulated
	 * result will notify the returned {@link Stream}. A {@link Function} taking a {@link reactor.fn.support.Reduce}
	 * argument must be passed to process each pair formed of the last accumulated result and a new value to be processed.
	 *
	 * @param fn      The reduce function
	 * @param initial The initial accumulated result value e.g. an empty list.
	 * @param <V>     The type of the object returned by reactor reply.
	 * @return The new {@link Stream}.
	 */
	public <V> Stream<V> reduce(final Function<Reduce<T, V>, V> fn, V initial) {
		Assert.notNull(fn);
		final AtomicReference<V> lastValue = new AtomicReference<V>(initial);
		final Stream<V> c = (Stream<V>) this.assignComposable(getObservable());

		final long _expectedAcceptCount;
		synchronized (monitor) {
			_expectedAcceptCount = getExpectedAcceptCount();
		}

		c.setExpectedAcceptCount(_expectedAcceptCount < 0 ? _expectedAcceptCount : 1);
		when(lastSelector, new Consumer<T>() {
			@Override
			public void accept(T t) {
				c.accept(lastValue.get());
			}
		});

		consume(new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					Reduce<T, V> r = new Reduce<T, V>(lastValue.get(), value);
					lastValue.set(fn.apply(r));
					if (_expectedAcceptCount < 0) {
						c.accept(lastValue.get());
					}
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		});

		return c;
	}

	/**
	 * Accumulate a result until expected accept count has been reached - If this limit hasn't been set, each accumulated
	 * result will notify the returned {@link Stream}. Will automatically generate a collection formed from composable
	 * streamed results, until accept count is reached.
	 *
	 * @return The new {@link Stream}.
	 */
	public Stream<List<T>> reduce() {
		return reduce(new Function<Reduce<T, List<T>>, List<T>>() {
			@Override
			public List<T> apply(Reduce<T, List<T>> reducer) {
				reducer.getLastValue().add(reducer.getNextValue());
				return reducer.getLastValue();
			}
		}, new ArrayList<T>());
	}

	/**
	 * Take {@param count} number of values and send lastSelector event after {@param count} iterations
	 *
	 * @param count Number of values to accept
	 * @return The new {@link Stream}.
	 */
	public Stream<T> take(long count) {
		final Stream<T> c = (Stream<T>) this.assignComposable(getObservable());
		c.setExpectedAcceptCount(count);
		consume(c);

		return c;
	}

	/**
	 * Accumulate a result until expected accept count has been reached - If this limit hasn't been set, each accumulated
	 * result will notify the returned {@link Stream}. A {@link Function} taking a {@link Reduce} argument must be
	 * passed to process each pair formed of the last accumulated result and a new value to be processed.
	 *
	 * @param fn  The reduce function
	 * @param <V> The type of the object returned by reactor reply.
	 * @return The new {@link Stream}.
	 */
	public <V> Stream<V> reduce(final Function<Reduce<T, V>, V> fn) {
		return reduce(fn, null);
	}

	/**
	 * Create a new {@link Stream} that is linked to the parent through the given {@code key} and {@link Observable}.
	 * When the parent's {@link #accept(Object)} is invoked, its value is wrapped into an {@link Event} and passed to
	 * {@link Observable#notify (reactor.Event.wrap)} along with the given {@code key}. After the event is being propagated
	 * to the reactor consumers, the new composition expects {@param <V>} replies to be returned.
	 *
	 * @param key        The key to notify
	 * @param observable The observable to notify
	 * @param <V>        The type of the object returned by reactor reply.
	 * @return The new {@link Stream}.
	 */
	public <V> Stream<V> map(final Object key, final Observable observable) {
		Assert.notNull(observable);
		final Stream<V> c = (Stream<V>) this.assignComposable(observable);
		c.setExpectedAcceptCount(-1);
		final Object replyTo = new Object();

		observable.on($(replyTo), new Consumer<Event<V>>() {
			@Override
			public void accept(Event<V> event) {
				try {
					c.accept(event.getData());
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		});

		consume(new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					Event<?> event = Event.class.isAssignableFrom(value.getClass()) ? (Event<?>) value : Event.wrap(value);
					event.setReplyTo(replyTo);
					observable.send(key, event);
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		});

		return c;
	}

	@Override
	protected Stream createComposable(Observable src) {
		return new Stream(getEnvironment(), createReactor(src));
	}


	protected final void notifyFirst(Event<?> event) {
		getObservable().notify(firstKey, event);
	}

	protected final void notifyLast(Event<?> event) {
		getObservable().notify(lastKey, event);
	}

	@Override
	public <E extends Throwable> Stream<T> when(Class<E> exceptionType, Consumer<E> onError) {
		return (Stream<T>) super.when(exceptionType, onError);
	}

	@Override
	public Stream<T> consume(Consumer<T> consumer) {
		return (Stream<T>) super.consume(consumer);
	}

	@Override
	public Stream<T> consume(Object key, Observable observable) {
		return (Stream<T>) super.consume(key, observable);
	}

	@Override
	public Stream<T> consume(Composable<T> composable) {
		return (Stream<T>) super.consume(composable);
	}

	@Override
	public <V> Stream<V> map(Function<T, V> fn) {
		return (Stream<V>) super.map(fn);
	}

	@Override
	public Stream<T> filter(Function<T, Boolean> fn) {
		return (Stream<T>) super.filter(fn);
	}

	/**
	 * Build a {@link Stream} based on the given values, {@link Dispatcher dispatcher}, and {@link Reactor reactor}.
	 *
	 * @param <T> The type of the values.
	 */
	public static class Spec<T> extends ComponentSpec<Spec<T>, Stream<T>> {

		protected final Iterable<T> values;

		public Spec(Iterable<T> values) {
			this.values = values;
		}

		@Override
		protected Stream<T> configure(final Reactor reactor) {

			final Stream<T> comp;
			if (values != null) {
				comp = new DeferredStream<T>(env, reactor, values);
			} else {
				comp = new DeferredStream<T>(env, reactor, -1);
			}
			return comp;
		}
	}

	protected static class DeferredStream<T> extends Stream<T> {
		private final Object stateMonitor = new Object();
		protected final Iterable<T> values;
		protected AcceptState acceptState = AcceptState.DELAYED;

		protected DeferredStream(Environment env, Observable src, Iterable<T> values) {
			super(env, src);
			this.values = values;
			if (values instanceof Collection) {
				setExpectedAcceptCount((((Collection<?>) values).size()));
			}
		}

		protected DeferredStream(Environment env, Observable src, long length) {
			super(env, src);
			this.values = null;
			setExpectedAcceptCount(length);
		}

		@Override
		public void accept(Throwable error) {
			setError(error);
			notifyError(error);
		}

		private void acceptValues(Iterable<T> values) {
			for (T initValue : values) {
				accept(initValue);
			}
		}

		@Override
		public void accept(T value) {
			boolean notifyFirst = false;
			boolean notifyLast = false;
			boolean init = false;

			if (values != null) {
				synchronized (this.stateMonitor) {
					if (acceptState == AcceptState.DELAYED) {
						acceptState = AcceptState.ACCEPTING;
						init = true;
					}
				}

				if (init) {
					acceptValues(values);
				}
			}

			synchronized (monitor) {
				setValue(value);

				if (isFirst()) {
					notifyFirst = true;
				}

				if (acceptCountReached()) {
					notifyLast = true;
					monitor.notifyAll();
				}
			}

			Event<T> ev = Event.wrap(value);

			if (notifyFirst) {
				notifyFirst(ev);
			}

			notifyAccept(ev);

			if (notifyLast) {
				notifyLast(ev);
			}

			synchronized (this.stateMonitor) {
				acceptState = AcceptState.ACCEPTED;
			}

		}

		@Override
		public T await(long timeout, TimeUnit unit) throws InterruptedException {
			delayedAccept();
			return super.await(timeout, unit);
		}

		@Override
		public T get() {
			delayedAccept();
			return super.get();
		}

		@Override
		protected boolean isComplete() {
			if (values != null) {
				boolean init;
				synchronized (this.stateMonitor) {
					init = acceptState == AcceptState.DELAYED;
				}
				if (init) {
					acceptValues(values);
					synchronized (this.stateMonitor) {
						acceptState = AcceptState.ACCEPTED;
						stateMonitor.notifyAll();
					}
				}

			}
			return super.isComplete();
		}

		@Override
		protected Stream createComposable(Observable src) {
			final DeferredStream<T> self = this;
			final DeferredStream c =
					new DeferredStream(getEnvironment(), createReactor(src), self.getExpectedAcceptCount()) {
						@Override
						protected void delayedAccept() {
							self.delayedAccept();
						}
					};
			forwardError(c);
			return c;
		}

		protected void delayedAccept() {
			doAccept(null, null, null);
		}

		protected void doAccept(Throwable localError, Iterable<T> localValues, T localValue) {

			boolean acceptRequired = false;

			synchronized (this.stateMonitor) {
				if (acceptState == AcceptState.ACCEPTED) {
					return;
				} else if (acceptState == AcceptState.DELAYED) {
					if (localError == null && localValue == null && localValues == null) {
						synchronized (this.monitor) {
							localError = getError();
							localValue = getValue();
							localValues = values;
						}
					}
					acceptState = AcceptState.ACCEPTING;
					acceptRequired = true;
				} else {
					while (acceptState == AcceptState.ACCEPTING) {
						try {
							stateMonitor.wait();
						} catch (InterruptedException ie) {
							Thread.currentThread().interrupt();
							break;
						}
					}
				}
			}

			if (acceptRequired) {
				if (null != localError) {
					accept(localError);
				} else if (null != localValues) {
					acceptValues(localValues);
				} else if (null != localValue) {
					accept(localValue);
				}
				synchronized (stateMonitor) {
					acceptState = AcceptState.ACCEPTED;
					stateMonitor.notifyAll();
				}
			}
		}

		private static enum AcceptState {
			DELAYED, ACCEPTING, ACCEPTED;
		}
	}

}