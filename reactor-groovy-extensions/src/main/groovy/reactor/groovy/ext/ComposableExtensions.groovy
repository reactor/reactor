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

package reactor.groovy.ext

import groovy.transform.CompileStatic
import groovy.transform.TypeCheckingMode
import reactor.core.composable.Composable
import reactor.core.composable.Deferred
import reactor.core.composable.Promise
import reactor.core.composable.Stream
import reactor.function.*
import reactor.groovy.support.*
import reactor.tuple.Tuple2

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
	static <T, X extends Composable<T>> X to(final X selfType, final Object key,
	                                         final reactor.core.Observable observable) {
		selfType.consume key, observable
	}

	/**
	 * Closure converters
	 */
	static <T, V> Composable<V> map(final Deferred selfType, final Closure<V> closure) {
		selfType.compose().map new ClosureFunction<T, V>(closure)
	}

	static <T, V> Stream<V> map(final Stream<T> selfType, final Closure<V> closure) {
		selfType.map new ClosureFunction<T, V>(closure)
	}

	static <T, V> Promise<V> map(final Promise<T> selfType, final Closure<V> closure) {
		selfType.map new ClosureFunction<T, V>(closure)
	}

	static <T> Stream<T> consume(final Stream<T> selfType, final Closure closure) {
		selfType.consume new ClosureConsumer<T>(closure)
	}

	static <T, V, C extends Composable<V>> Stream<V> mapMany(final Stream<T> selfType, final Closure<C> closure) {
		selfType.mapMany new ClosureFunction<T, C>(closure)
	}

	static <T, V, C extends Promise<V>> Promise<V> bind(final Promise<T> selfType, final Closure<C> closure) {
		selfType.mapMany new ClosureFunction<T, C>(closure)
	}

	static <T> Composable<T> consume(final Deferred selfType, final Closure closure) {
		selfType.compose().consume new ClosureConsumer<T>(closure)
	}

	static <T> Promise<T> consume(final Promise<T> selfType, final Closure closure) {
		selfType.consume new ClosureConsumer<T>(closure)
	}

	static <T> Stream<T> filter(final Stream<T> selfType, final Closure<Boolean> closure) {
		selfType.filter new ClosurePredicate<T>(closure)
	}

	static <T> Composable<T> filter(final Deferred selfType, final Closure<Boolean> closure) {
		selfType.compose().filter new ClosurePredicate<T>(closure)
	}

	static <T> Promise<T> filter(final Promise<T> selfType, final Closure<Boolean> closure) {
		selfType.filter new ClosurePredicate<T>(closure)
	}

	static <T, E> Stream<T> when(final Stream<T> selfType, final Class<E> exceptionType, final Closure closure) {
		selfType.when exceptionType, new ClosureConsumer<E>(closure)
	}

	static <T, E> Composable<T> when(final Deferred selfType, final Class<E> exceptionType, final Closure closure) {
		selfType.compose().when exceptionType, new ClosureConsumer<E>(closure)
	}

	static <T, E> Promise<T> when(final Promise<T> selfType, final Class<E> exceptionType, final Closure closure) {
		selfType.when exceptionType, new ClosureConsumer<E>(closure)
	}

	@CompileStatic(TypeCheckingMode.SKIP)
	static <T, V> Stream<V> reduce(final Stream<T> selfType, final Closure<V> closure, V initial = null) {
		reduce selfType, closure, initial ? Functions.<V>supplier((V)initial) : (Supplier<V>)null
	}

	static <T, V> Stream<V> reduce(final Stream<T> selfType, final Closure<V> closure, Closure<V> initial) {
		reduce selfType, closure, new ClosureSupplier(initial)
	}

	@CompileStatic(TypeCheckingMode.SKIP)
	static <T, V> Stream<V> reduce(final Stream<T> selfType, final Closure<V> closure, Supplier<V> initial) {
		selfType.reduce new ClosureReduce<T, V>(closure), initial
	}

	static <T> Promise<T> onError(final Promise<T> selfType, final Closure closure) {
		selfType.onError new ClosureConsumer<Throwable>(closure)
	}

	static <T> Promise<T> onComplete(final Promise<T> selfType, final Closure closure) {
		selfType.onComplete new ClosureConsumer<Promise<T>>(closure)
	}

	static <T> Promise<T> onSuccess(final Promise<T> selfType, final Closure closure) {
		selfType.onSuccess new ClosureConsumer<T>(closure)
	}

	static <T, V> Promise<V> then(final Promise<T> selfType, final Closure<V> closureSuccess,
	                              final Closure closureError = null) {
		selfType.then(new ClosureFunction<T, V>(closureSuccess), closureError ?
				new ClosureConsumer<Throwable>(closureError) : null)
	}

	/**
	 * Operator overloading
	 */

	@CompileStatic(TypeCheckingMode.SKIP)
	static <T, V> Stream<V> mod(final Stream<T> selfType, final Function<Tuple2<T, V>, V> other) {
		selfType.reduce other, (Supplier<V>)null
	}

	static <T, V> Stream<V> mod(final Stream<T> selfType, final Closure<V> other) {
		reduce selfType, other
	}

	//Mapping
	static <T, V> Stream<V> or(final Stream<T> selfType, final Function<T, V> other) {
		selfType.map other
	}

	static <T, V> Stream<V> or(final Stream<T> selfType, final Closure<V> other) {
		map selfType, other
	}

	static <T, V> Composable<V> or(final Deferred selfType, final Function<T, V> other) {
		selfType.compose().map other
	}

	static <T, V> Composable<V> or(final Deferred selfType, final Closure<V> other) {
		map selfType, other
	}

	static <T, V> Promise<V> or(final Promise<T> selfType, final Function<T, V> other) {
		selfType.then other, (Consumer<Throwable>) null
	}

	static <T, V> Promise<V> or(final Promise<T> selfType, final Closure<V> other) {
		then selfType, other
	}


	//Filtering
	static <T> Stream<T> and(final Stream<T> selfType, final Closure<Boolean> other) {
		filter selfType, other
	}

	static <T> Promise<T> and(final Promise<T> selfType, final Closure<Boolean> other) {
		filter selfType, other
	}

	static <T> Composable<T> and(final Deferred selfType, final Closure<Boolean> other) {
		filter selfType, other
	}

	static <T> Stream<T> and(final Stream<T> selfType, final Predicate<T> other) {
		selfType.filter other
	}

	static <T> Promise<T> and(final Promise<T> selfType, final Predicate<T> other) {
		selfType.filter other
	}

	static <T> Composable<T> and(final Deferred selfType, final Predicate<T> other) {
		selfType.compose().filter other
	}


	//Consuming
	static <T> Stream<T> leftShift(final Stream<T> selfType, final Consumer<T> other) {
		selfType.consume other
	}

	static <T> Composable<T> leftShift(final Deferred selfType, final Consumer<T> other) {
		selfType.compose().consume other
	}

	static <T> Stream<T> leftShift(final Stream<T> selfType, final Closure other) {
		consume selfType, other
	}

	static <T> Composable<T> leftShift(final Deferred selfType, final Closure other) {
		consume selfType, other
	}

	static <T> Promise<T> leftShift(final Promise<T> selfType, final Consumer<T> other) {
		selfType.onSuccess other
	}

	static <T> Promise<T> leftShift(final Promise<T> selfType, final Closure other) {
		onSuccess selfType, other
	}

	//Accept
	static <T> Consumer<T> leftShift(final Consumer<T> selfType, T value) {
		selfType.accept value
		selfType
	}

	static <T, X extends Composable<T>> Deferred<T, X> leftShift(final Deferred<T, X> selfType, T value) {
		selfType.accept value
		selfType
	}

}