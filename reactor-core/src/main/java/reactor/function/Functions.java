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

package reactor.function;

import java.lang.reflect.Constructor;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.tuple.Tuple;

/**
 * Helper methods to provide syntax sugar for working with functional components in Reactor.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public abstract class Functions {

	/**
	 * Schedule an arbitrary {@link Consumer} to be executed on the given {@link reactor.core.Observable}, passing the given {@link
	 * reactor.event.Event}.
	 *
	 * @param consumer   The {@link Consumer} to invoke.
	 * @param data       The data to pass to the consumer.
	 * @param observable The {@literal Observable} that will be used to invoke the {@literal Consumer}
	 * @param <T>        The type of the data.
	 */
	public static <T> void schedule(final Consumer<T> consumer, T data, Observable observable) {
		observable.notify(Event.wrap(Tuple.of(consumer, data)));
	}

	/**
	 * Wrap the given {@link java.util.concurrent.Callable} and compose a new {@link reactor.function.Function}.
	 *
	 * @param c The {@link java.util.concurrent.Callable}.
	 * @return An {@link reactor.function.Consumer} that executes the {@link java.util.concurrent.Callable}.
	 */
	public static <T> Function<? extends Event<T>, T> function(final Callable<T> c) {
		return new Function<Event<T>, T>() {
			@Override
			public T apply(Event<T> o) {
				try {
					return c.call();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}

	/**
	 * Wrap the given {@link Runnable} and compose a new {@link reactor.function.Consumer}.
	 *
	 * @param r The {@link Runnable}.
	 * @return An {@link reactor.function.Consumer} that executes the {@link Runnable}.
	 */
	public static <T> Consumer<T> consumer(final Runnable r) {
		return new Consumer<T>() {
			@Override
			public void accept(T t) {
				r.run();
			}
		};
	}

	/**
	 * Creates a {@code Supplier} that will always return the given {@code value}.
	 *
	 * @param value the value to be supplied
	 * @return the supplier for the value
	 */
	public static <T> Supplier<T> supplier(final T value) {
		return new Supplier<T>() {
			@Override
			public T get() {
				return value;
			}
		};
	}

	/**
	 * Creates a {@code Supplier} that will return a new instance of {@code type} each time
	 * it's called.
	 *
	 * @param type The type to create
	 *
	 * @return The supplier that will create instances
	 *
	 * @throws IllegalArgumentException if {@code type} does not have a zero-args constructor
	 */
	public static <T> Supplier<T> supplier(final Class<T> type) {
		try {
			final Constructor<T> ctor = type.getConstructor();
			return new Supplier<T>() {
				@Override
				public T get() {
					try {
						return ctor.newInstance();
					} catch (Exception e) {
						throw new IllegalStateException(e.getMessage(), e);
					}
				}
			};
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	/**
	 * Creates a {@code Supplier} that will {@link Callable#call call} the {@code callable}
	 * each time it's asked for a value.
	 *
	 * @param callable The {@link Callable}.
	 * @return A {@link Supplier} that executes the {@link Callable}.
	 */
	public static <T> Supplier<T> supplier(final Callable<T> callable) {
		return new Supplier<T>() {
			@Override
			public T get() {
				try {
					return callable.call();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}

	/**
	 * Creates a {@code Supplier} that will {@link Future#get get} its value from the
	 * {@code future} each time it's asked for a value.
	 *
	 * @param future The future to get values from
	 * @return A {@link reactor.function.Supplier} that gets its values from the Future
	 */
	public static <T> Supplier<T> supplier(final Future<T> future) {
		return new Supplier<T>() {
			@Override
			public T get() {
				try {
					return future.get();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}

}
