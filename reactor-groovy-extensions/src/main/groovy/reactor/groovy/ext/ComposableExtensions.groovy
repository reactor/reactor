/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
import reactor.bus.Observable
import reactor.fn.BiFunction
import reactor.fn.Consumer
import reactor.fn.Function
import reactor.fn.Predicate
import reactor.fn.tuple.Tuple2
import reactor.rx.Controls
import reactor.rx.Promise
import reactor.rx.Stream
import reactor.rx.action.Action

/**
 * Glue for Groovy closures and operator overloading applied to Stream, Composable,
 * Promise and Deferred.
 * Also gives convenient Deferred handling by providing map/filter/consume operations
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@CompileStatic
class ComposableExtensions {

	/**
	 * Alias
	 */
	static <T, X extends Stream<T>> Controls to(final Stream<T> selfType, final Object key,
	                                         final Observable observable) {
		selfType.notify key, observable
	}


	/**
	 * Operator overloading
	 */

	static <T> Stream<T> mod(final Stream<T> selfType, final BiFunction<T, T, T> other) {
		selfType.reduce other
	}

	//Mapping
	static <T, V> Stream<V> or(final Stream<T> selfType, final Function<T, V> other) {
		selfType.map other
	}

	static <T, V> Stream<V> or(final Promise<T> selfType, final Function<T, V> other) {
		selfType.stream().map(other)
	}

	//Filtering
	static <T> Stream<T> and(final Stream<T> selfType, final Predicate<T> other) {
		selfType.filter other
	}

	static <T> Stream<T> and(final Promise<T> selfType, final Predicate<T> other) {
		selfType.stream().filter(other)
	}


	//Consuming
	static <T> Controls leftShift(final Stream<T> selfType, final Consumer<T> other) {
		selfType.consume other
	}

	static <T> Promise<T> leftShift(final Promise<T> selfType, final Consumer<T> other) {
		selfType.onSuccess other
	}

	//Consuming
	static <T> Action<?, T> leftShift(final Action<?, T> selfType, final T value) {
		selfType.onNext(value)
	}

	static <T> Promise<T> leftShift(final Promise<T> selfType, final T value) {
		selfType.onNext value
	}

}