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

import reactor.fn.Consumer;
import reactor.fn.Observable;

/**
 * A {@literal Composable} is a specific type of {@link Future} implementing {@link Consumer} in order to provide
 * public scoped accept methods. A Composable can push and pull data to other components that must wait on the data to
 * become available.
 *
 * @param <T> The {@link Composable}  output type.
 * @author Stephane Maldini
 */
public abstract class Composable<T> extends Future<T> implements Consumer<T> {


	/**
	 * Create a {@link Composable} that uses the given {@link reactor.core.Reactor} for publishing events internally.
	 *
	 * @param observable The {@link reactor.core.Reactor} to use.
	 */
	protected Composable(Environment env, Observable observable) {
		super(env, observable);
	}

	/**
	 * Register a {@link Composable} that will be invoked whenever {@link #accept(Object)} or {@link #accept (Throwable)}
	 * are called.
	 *
	 * @param composable The composable to invoke.
	 * @return {@literal this}
	 * @see {@link #accept(Object)}
	 */
	public Composable<T> consume(Composable<T> composable) {
		consume((Consumer<T>) composable);
		forwardError(composable);
		return this;
	}


	/**
	 * Provide a read-only future that no longer accepts user values
	 */
	public Future<T> future() {
		final Future<T> c = super.createFuture(getObservable());
		synchronized (monitor) {
			c.doSetExpectedAcceptCount(getExpectedAcceptCount());
			if (null != getValue()) {
				c.setValue(getValue());
			}
			if (null != getError()) {
				c.setError(getError());
			}
		}
		when(Throwable.class, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				c.internalAccept(throwable);
			}
		})
				.consume(new Consumer<T>() {
					@Override
					public void accept(T t) {
						c.internalAccept(t);
					}
				});
		return c;
	}

	/**
	 * Trigger composition with an exception to be processed by dedicated consumers
	 *
	 * @param error The exception
	 */
	public void accept(Throwable error) {
		internalAccept(error);
		notifyError(error);
	}

	/**
	 * Trigger composition with a value to be processed by dedicated consumers
	 *
	 * @param value The exception
	 */
	@Override
	public void accept(T value) {
		internalAccept(value);
	}


	protected Composable<T> forwardError(final Composable<?> composable) {
		if (composable.getObservable() == getObservable()) {
			return this;
		}
		when(Throwable.class, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				composable.accept(t);
				composable.decreaseAcceptLength();
			}
		});
		return this;
	}

}
