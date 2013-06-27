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
import reactor.fn.support.EventConsumer;
import reactor.fn.support.NotifyConsumer;
import reactor.fn.tuples.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import java.util.concurrent.locks.ReentrantLock;

import static reactor.fn.Functions.$;

/**
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public abstract class Composable<T> {

	protected final ReentrantLock lock = new ReentrantLock();

	private final Tuple2<Selector, Object> accept = $();

	private final Environment env;
	private final Observable  events;

	private volatile long acceptCount = 0l;
	private volatile long errorCount  = 0l;

	protected Composable(Environment env,
											 @Nonnull Observable events) {
		Assert.notNull(events, "Events Observable cannot be null.");
		this.env = env;
		this.events = events;

	}

	public Composable<T> consume(final Consumer<T> consumer) {
		this.events.on(accept.getT1(), new EventConsumer<T>(consumer));
		return this;
	}

	public Composable<T> consume(final Object key, final Observable observable) {
		consume(new NotifyConsumer<T>(key, observable));
		return this;
	}

	public <E extends Throwable> Composable<T> when(Class<E> exceptionType, Consumer<E> onError) {
		this.events.on(Fn.T(exceptionType), new EventConsumer<E>(onError));
		return this;
	}

	public <V> Composable<V> map(final Function<T, V> fn) {
		Assert.notNull(fn, "Map function cannot be null.");
		final Deferred<V, ? extends Composable<V>> d = createDeferred();
		consume(new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					V val = fn.apply(value);
					d.accept(val);
				} catch (Throwable e) {
					d.accept(e);
				}
			}
		});
		return d.compose();
	}

	public Composable<T> filter(final Predicate<T> p) {
		Assert.notNull(p, "Filter predicate cannot be null.");
		final Deferred<T, ? extends Composable<T>> d = createDeferred();
		consume(new Consumer<T>() {
			@Override
			public void accept(T value) {
				boolean b = p.test(value);
				if (b) {
					d.accept(value);
				} else {
					d.accept(new IllegalArgumentException(String.format("%s failed a predicate test.", value)));
				}
			}
		});
		return d.compose();
	}

	public long getAcceptCount() {
		lock.lock();
		try {
			return acceptCount;
		} finally {
			lock.unlock();
		}
	}

	public long getErrorCount() {
		lock.lock();
		try {
			return errorCount;
		} finally {
			lock.unlock();
		}
	}

	void notifyValue(T value) {
		lock.lock();
		try {
			acceptCount++;
			valueAccepted(value);
		} finally {
			lock.unlock();
		}
		events.notify(accept.getT2(), Event.wrap(value));
	}

	void notifyError(Throwable error) {
		lock.lock();
		try {
			errorCount++;
			errorAccepted(error);
		} finally {
			lock.unlock();
		}
		events.notify(error.getClass(), Event.wrap(error));
	}

	protected abstract <V, C extends Composable<V>> Deferred<V, C> createDeferred();

	protected abstract void errorAccepted(Throwable error);

	protected abstract void valueAccepted(T value);

	protected void cascadeErrors(final Composable<?> composable) {
		when(Throwable.class, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				composable.notifyError(t);
			}
		});
	}

	protected Environment getEnvironment() {
		return env;
	}

	protected Observable getObservable() {
		return events;
	}

}
