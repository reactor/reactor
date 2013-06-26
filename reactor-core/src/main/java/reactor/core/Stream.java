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

package reactor.core;

import reactor.Fn;
import reactor.fn.*;
import reactor.fn.selector.Selector;
import reactor.fn.tuples.Tuple;
import reactor.fn.tuples.Tuple2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static reactor.fn.Functions.$;

/**
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class Stream<T> extends Composable<T> implements Supplier<Stream<T>> {

	private final Tuple2<Selector, Object> first = $();
	private final Tuple2<Selector, Object> last  = $();
	private final int         batchSize;
	private final Iterable<T> values;
	private final Stream<?>   parent;

	Stream(@Nonnull Environment env,
				 @Nonnull Observable events,
				 int batchSize,
				 @Nullable Iterable<T> values,
				 Stream<?> parent) {
		super(env, events);
		this.batchSize = batchSize;
		this.values = values;
		this.parent = parent;
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
	public <E extends Throwable> Stream<T> when(Class<E> exceptionType, Consumer<E> onError) {
		return (Stream<T>) super.when(exceptionType, onError);
	}

	@Override
	public <V> Stream<V> map(Function<T, V> fn) {
		return (Stream<V>) super.map(fn);
	}

	@Override
	public Stream<T> filter(Predicate<T> p) {
		return (Stream<T>) super.filter(p);
	}

	public Stream<T> first() {
		final Deferred<T, Stream<T>> d = createDeferred();
		getObservable().on(first.getT1(), new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				d.accept(ev.getData());
			}
		});
		return d.compose();
	}

	public Stream<T> last() {
		final Deferred<T, Stream<T>> d = createDeferred();
		getObservable().on(last.getT1(), new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				d.accept(ev.getData());
			}
		});
		return d.compose();
	}

	public Stream<List<T>> batch(final int batchSize) {
		final Deferred<List<T>, Stream<List<T>>> d = createDeferred(batchSize);

		consume(new Consumer<T>() {
			List<T> batchValues = new ArrayList<T>(batchSize);

			@Override
			public void accept(T value) {
				batchValues.add(value);
				long accepted = getAcceptCount();
				if (accepted % batchSize == 0 || accepted == Stream.this.batchSize) {
					// This List might be accessed from another thread, depending on the Dispatcher in use, so copy it.
					List<T> copy = new ArrayList<T>(batchSize);
					copy.addAll(batchValues);
					d.accept(copy);
					batchValues.clear();
				}
			}
		});

		return d.compose();
	}

	public boolean isBatch() {
		return batchSize > 0;
	}

	public <A> Stream<A> reduce(Function<Tuple2<T, A>, A> fn, A initial) {
		return reduce(fn, Fn.supplier(initial));
	}

	public <A> Stream<A> reduce(final Function<Tuple2<T, A>, A> fn, final Supplier<A> accumulators) {
		final Deferred<A, Stream<A>> d = createDeferred();

		consume(new Consumer<T>() {
			private A acc;

			@Override
			public void accept(T value) {
				long accepted = (isBatch() ? getAcceptCount() % batchSize : getAcceptCount());
				if (accepted == 1) {
					acc = (null != accumulators ? accumulators.get() : null);
				}
				acc = fn.apply(Tuple.of(value, acc));
				if (isBatch() && (getAcceptCount() % batchSize == 0)) {
					d.accept(acc);
				} else {
					d.accept(acc);
				}
			}
		});

		return d.compose();
	}

	public <A> Stream<A> reduce(final Function<Tuple2<T, A>, A> fn) {
		return reduce(fn, (Supplier<A>) null);
	}

	@Override
	public Stream<T> get() {
		if (null != parent) {
			parent.get();
		}
		if (null != values) {
			for (T val : values) {
				notifyValue(val);
			}
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
		return createDeferred(batchSize);
	}

	@SuppressWarnings("unchecked")
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred(int batchSize) {
		return (Deferred<V, C>) new Deferred.StreamSpec<V>()
				.using(getEnvironment())
				.using((Reactor) getObservable())
				.link(this)
				.batch(batchSize)
				.get();
	}

	@Override
	protected void errorAccepted(Throwable error) {
	}

	@Override
	protected void valueAccepted(final T value) {
		if (!isBatch()) {
			return;
		}
		long accepted = getAcceptCount() % batchSize;
		if (accepted == 1) {
			getObservable().notify(first.getT2(), Event.wrap(value));
		} else if (accepted == 0) {
			getObservable().notify(last.getT2(), Event.wrap(value));
		}
	}

	@Override
	public String toString() {
		return "Stream{" +
				"batchSize=" + batchSize +
				", values=" + values +
				", parent=" + parent +
				'}';
	}
}