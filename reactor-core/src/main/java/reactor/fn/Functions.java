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

package reactor.fn;

import reactor.fn.selector.*;
import reactor.fn.tuples.Tuple;
import reactor.fn.tuples.Tuple2;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Helper methods to provide syntax sugar for working with functional components in Reactor.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Functions {

	/**
	 * Creates an anonymous {@link reactor.fn.selector.Selector}.
	 *
	 * @return A {@link reactor.fn.tuples.Tuple} containing the {@link reactor.fn.selector.Selector} and the object key.
	 * @see {@link reactor.fn.selector.ObjectSelector}
	 */
	public static Tuple2<Selector, Object> $() {
		Object obj = new Object();
		return new Tuple2<Selector, Object>($(obj), obj);
	}

	/**
	 * Creates a {@link Selector} based on the given object.
	 *
	 * @param obj Can be anything.
	 * @return The new {@link Selector}.
	 * @see {@link reactor.fn.selector.ObjectSelector}
	 */
	public static <T> Selector $(T obj) {
		return new ObjectSelector<T>(obj);
	}

	/**
	 * Creates a {@link Selector} based on the given regular expression.
	 *
	 * @param regex The regular expression to compile.
	 * @return The new {@link Selector}.
	 * @see {@link reactor.fn.selector.RegexSelector}
	 */
	public static Selector R(String regex) {
		return new RegexSelector(regex);
	}

	/**
	 * Creates a {@link Selector} based on the given class type and only matches if the other Selector against which this
	 * is compared is assignable according to {@link Class#isAssignableFrom(Class)}.
	 *
	 * @param supertype The supertype to compare.
	 * @return The new {@link Selector}.
	 * @see {@link reactor.fn.selector.ClassSelector}
	 */
	public static Selector T(Class<?> supertype) {
		return new ClassSelector(supertype);
	}

	/**
	 * Creates a {@link Selector} based on a URI template.
	 *
	 * @param uriTemplate The URI template to compile.
	 * @return The new {@link Selector}.
	 * @see {@link reactor.fn.selector.UriTemplate}
	 * @see {@link reactor.fn.selector.UriTemplateSelector}
	 */
	public static Selector U(String uriTemplate) {
		return new UriTemplateSelector(uriTemplate);
	}

	/**
	 * Schedule an arbitrary {@link Consumer} to be executed on the given {@link Observable}, passing the given {@link
	 * Event}.
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
	 * Wrap the given {@link java.util.concurrent.Callable} and compose a new {@link reactor.fn.Function}.
	 *
	 * @param c The {@link java.util.concurrent.Callable}.
	 * @return An {@link reactor.fn.Consumer} that executes the {@link java.util.concurrent.Callable}.
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
	 * Wrap the given {@link Runnable} and compose a new {@link reactor.fn.Consumer}.
	 *
	 * @param r The {@link Runnable}.
	 * @return An {@link reactor.fn.Consumer} that executes the {@link Runnable}.
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
	 * Wrap the given {@link Callable} and compose a new {@link reactor.fn.Supplier}.
	 *
	 * @param r The {@link Callable}.
	 * @return An {@link reactor.fn.Supplier} that executes the {@link Callable}.
	 */
	public static <T> Supplier<T> supplier(final Callable<T> r) {
		return new Supplier<T>() {
			@Override
			public T get() {
				try {
					return r.call();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}

	/**
	 * Wrap the given {@link java.util.concurrent.Future} and compose a new {@link reactor.fn.Supplier}.
	 *
	 * @param r The {@link java.util.concurrent.Future}.
	 * @return An {@link reactor.fn.Supplier} that fetch the {@link java.util.concurrent.Future}.
	 */
	public static <T> Supplier<T> supplier(final Future<T> r) {
		return new Supplier<T>() {
			@Override
			public T get() {
				try {
					return r.get();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}

}
