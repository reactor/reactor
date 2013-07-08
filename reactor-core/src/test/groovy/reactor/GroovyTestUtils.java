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

package reactor;

import groovy.lang.Closure;
import groovy.lang.GString;
import reactor.event.Event;
import reactor.function.*;
import reactor.event.selector.ObjectSelector;
import reactor.event.selector.Selector;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class GroovyTestUtils {

	protected GroovyTestUtils() {
	}

	public static Selector $(long l) {
		return new ObjectSelector<Long>(l);
	}

	public static Selector $(String s) {
		return new ObjectSelector<String>(s);
	}

	public static Selector $(GString s) {
		return new ObjectSelector<String>(s.toString());
	}

	@SuppressWarnings({"rawtypes"})
	public static Consumer<String> stringHandler(final Closure cl) {
		return new Consumer<String>() {
			public void accept(String s) {
				cl.call(s);
			}
		};
	}

	@SuppressWarnings({"rawtypes"})
	public static <T> Consumer consumer(final Closure cl) {
		return new Consumer<T>() {
			Class[] argTypes = cl.getParameterTypes();

			@Override
			@SuppressWarnings({"unchecked"})
			public void accept(Object arg) {
				if (argTypes.length < 1) {
					cl.call();
					return;
				}
				if (null != arg
						&& argTypes[0] != Object.class
						&& !argTypes[0].isAssignableFrom(arg.getClass())
						&& arg instanceof Event) {
					accept(((Event) arg).getData());
					return;
				}

				cl.call(arg);
			}
		};
	}

	public static <K, V> Function<K, V> function(final Closure<V> cl) {
		return new Function<K, V>() {
			Class<?>[] argTypes = cl.getParameterTypes();

			@Override
			@SuppressWarnings({"unchecked"})
			public V apply(K arg) {
				if (argTypes.length < 1) {
					return cl.call();
				}
				if (null != arg
						&& argTypes[0] != Object.class
						&& !argTypes[0].isAssignableFrom(arg.getClass())
						&& arg instanceof Event) {
					return apply((K) ((Event<?>) arg).getData());
				}

				return cl.call(arg);
			}
		};
	}
	public static <K> Predicate<K> predicate(final Closure<Boolean> cl) {
		return new Predicate<K>() {
			Class<?>[] argTypes = cl.getParameterTypes();

			@Override
			public boolean test(K arg) {
				if (argTypes.length < 1) {
					return cl.call();
				}
				if (null != arg
						&& argTypes[0] != Object.class
						&& !argTypes[0].isAssignableFrom(arg.getClass())
						&& arg instanceof Event) {
					return test((K) ((Event<?>) arg).getData());
				}

				return cl.call(arg);
			}
		};
	}

	public static <V> Supplier<V> supplier(final Closure<V> cl) {
		return new Supplier<V>() {

			@Override
			@SuppressWarnings({"unchecked"})
			public V get() {
				return cl.call();
			}
		};
	}

}
