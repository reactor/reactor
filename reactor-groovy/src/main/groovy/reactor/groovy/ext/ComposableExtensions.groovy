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

package reactor.groovy.ext

import groovy.transform.CompileStatic
import reactor.core.Composable
import reactor.core.Promise
import reactor.fn.Consumer
import reactor.fn.Function
import reactor.groovy.ClosureConsumer
import reactor.groovy.ClosureEventConsumer
import reactor.groovy.ClosureFunction

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@CompileStatic
class ComposableExtensions {

	static <T,V> Composable<V> map(final Composable<T> selfType, final Closure<V> closure) {
		or selfType, new ClosureFunction<T,V>(closure)
	}

	static <T> Composable<T> consume(final Composable<T> selfType, final Closure closure) {
		div selfType, new ClosureConsumer<T>(closure)
	}

	static <T> Composable<T> filter(final Composable<T> selfType, final Closure<Boolean> closure) {
		and selfType, new ClosureFunction<T,Boolean>(closure)
	}


	static <T,E> Composable<T> when(final Composable<T> selfType, final Class<E> exceptionType, final Closure closure) {
		selfType.when exceptionType, new ClosureConsumer<E>(closure)
	}


	static <T> Composable<T> div(final Composable<T> selfType, final Consumer<T> other) {
		selfType.consume other
	}

	static <T,V> Composable<V> or(final Composable<T> selfType, final Function<T,V> other) {
		selfType.map other
	}

	static <T,V> Composable<V> and(final Composable<T> selfType, final Function<T,Boolean> other) {
		selfType.filter other
	}


	static <T> Composable<T> leftShift(final Composable<T> selfType, T value) {
		selfType.accept value
		selfType
	}

	static <T> Promise<T> leftShift(final Promise<T> selfType, T value) {
		selfType.set value
	}
}
