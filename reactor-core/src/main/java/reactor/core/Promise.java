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
import reactor.fn.tuples.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

import static reactor.fn.Functions.$;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Promise<T> extends Composable<T> implements Supplier<T> {

	private final Object                   monitor       = new Object();
	private final Tuple2<Selector, Object> valueAccepted = $();
	private final Tuple2<Selector, Object> errorAccepted = $();

	private final long       defaultTimeout;
	private final Promise<?> parent;

	private State state = State.PENDING;
	private T         value;
	private Throwable error;

	Promise(Environment env,
	        @Nonnull Observable events,
	        @Nullable Promise<?> parent) {
		super(env, events);
		this.defaultTimeout = env != null ? env.getProperty("reactor.await.defaultTimeout", Long.class, 30000L) : 30000L;
		this.parent = parent;
	}

	public Promise<T> onComplete(final Consumer<Promise<T>> onComplete) {
		Consumer<T> consumer = new Consumer<T>() {
			@Override
			public void accept(T value) {
				onComplete.accept(Promise.this);
			}
		};
		if (isComplete()) {
			Fn.schedule(consumer, null, getObservable());
		} else {
			consume(consumer);
		}
		return this;
	}

	public Promise<T> onSuccess(final Consumer<T> onSuccess) {
		Consumer<T> consumer = new Consumer<T>() {
			@Override
			public void accept(T value) {
				onSuccess.accept(value);
			}
		};
		if (isComplete()) {
			Fn.schedule(consumer, null, getObservable());
		} else {
			consume(consumer);
		}
		return this;
	}

	public Promise<T> onError(final Consumer<Throwable> onError) {
		Consumer<T> consumer = new Consumer<T>() {
			@Override
			public void accept(T value) {
				onError.accept(error);
			}
		};
		if (isComplete()) {
			Fn.schedule(consumer, null, getObservable());
		} else {
			consume(consumer);
		}
		return this;
	}

	public Promise<T> then(Consumer<T> onSuccess, Consumer<Throwable> onError) {
		onSuccess(onSuccess);
		onError(onError);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <V> Promise<V> then(final Function<T, V> onSuccess, final Consumer<Throwable> onError) {
		final Deferred<V, Promise<V>> d = new Deferred.PromiseSpec<V>()
				.using(getEnvironment())
				.using((Reactor) getObservable())
				.get();
		Promise<V> p = d.compose().onError(onError);

		getObservable().on(valueAccepted.getT1(), new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				d.accept(onSuccess.apply(value));
			}
		});
		getObservable().on(errorAccepted.getT1(), new Consumer<Event<Throwable>>() {
			@Override
			public void accept(Event<Throwable> ev) {
				d.accept(ev.getData());
			}
		});
		return p;
	}

	public boolean isComplete() {
		lock.lock();
		try {
			return state != State.PENDING;
		} finally {
			lock.unlock();
		}
	}

	public boolean isPending() {
		lock.lock();
		try {
			return state == State.PENDING;
		} finally {
			lock.unlock();
		}
	}

	public boolean isSuccess() {
		lock.lock();
		try {
			return state == State.SUCCESS;
		} finally {
			lock.unlock();
		}
	}

	public boolean isError() {
		lock.lock();
		try {
			return state == State.FAILURE;
		} finally {
			lock.unlock();
		}
	}

	public T await() throws InterruptedException {
		return await(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	public T await(long timeout, TimeUnit unit) throws InterruptedException {
		if (!isPending()) {
			return get();
		}

		lock.lock();
		try {
			synchronized (monitor) {
				if (timeout >= 0) {
					long msTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
					long endTime = System.currentTimeMillis() + msTimeout;
					long now;
					while (state == State.PENDING && (now = System.currentTimeMillis()) < endTime) {
						this.monitor.wait(endTime - now);
					}
				} else {
					while (state == State.PENDING) {
						this.monitor.wait();
					}
				}
			}
		} finally {
			lock.unlock();
		}

		return get();
	}

	@Override
	public T get() {
		if (null != parent) {
			parent.get();
		}
		if (isSuccess()) {
			return value;
		} else {
			return null;
		}
	}

	public Throwable reason() {
		if (isError()) {
			return error;
		} else {
			return null;
		}
	}

	@Override
	public Promise<T> consume(Consumer<T> consumer) {
		return (Promise<T>) super.consume(consumer);
	}

	@Override
	public Promise<T> consume(Composable<T> composable) {
		return (Promise<T>) super.consume(composable);
	}

	@Override
	public Promise<T> consume(Object key, Observable observable) {
		return (Promise<T>) super.consume(key, observable);
	}

	@Override
	public <E extends Throwable> Promise<T> when(Class<E> exceptionType, Consumer<E> onError) {
		return (Promise<T>) super.when(exceptionType, onError);
	}

	@Override
	public <V> Promise<V> map(Function<T, V> fn) {
		return (Promise<V>) super.map(fn);
	}

	@Override
	public Promise<T> filter(Predicate<T> p) {
		return (Promise<T>) super.filter(p);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
		return (Deferred<V, C>) new Deferred.PromiseSpec<V>()
				.using(getEnvironment())
				.using((Reactor) getObservable())
				.link(this)
				.get();
	}

	@Override
	protected void errorAccepted(Throwable error) {
		lock.lock();
		try {
			assertPending();
			this.state = State.FAILURE;
			this.error = error;
			getObservable().notify(errorAccepted.getT2(), Event.wrap(error));
		} finally {
			lock.unlock();
		}
	}

	@Override
	protected void valueAccepted(T value) {
		lock.lock();
		try {
			assertPending();
			this.state = State.SUCCESS;
			this.value = value;
		} finally {
			lock.unlock();
		}
	}

	private void assertPending() {
		Assert.state(state == State.PENDING, "Promise has already completed.");
	}

	private enum State {
		PENDING, SUCCESS, FAILURE
	}

}