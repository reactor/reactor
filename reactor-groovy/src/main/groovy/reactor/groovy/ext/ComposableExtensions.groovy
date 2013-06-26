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
import reactor.core.Deferred
import reactor.core.Promise
import reactor.core.Stream
import reactor.fn.*
import reactor.fn.tuples.Tuple2
import reactor.groovy.support.ClosureConsumer
import reactor.groovy.support.ClosureFunction
import reactor.groovy.support.ClosurePredicate
import reactor.groovy.support.ClosureReduce
/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@CompileStatic
class ComposableExtensions {

  /**
   * Alias
   */
  static <T, V> Stream<V> to(final Stream<T> selfType, final Object key, final Observable observable) {
    selfType.consume key, observable
  }

  /**
   * Closure converters
   */
  static <T, V> Stream<V> map(final Stream<T> selfType, final Closure<V> closure) {
    selfType.map new ClosureFunction<T, V>(closure)
  }

  static <T> Stream<T> consume(final Stream<T> selfType, final Closure closure) {
    selfType.consume new ClosureConsumer<T>(closure)
  }

  static <T, V> Promise<V> map(final Promise<T> selfType, final Closure<V> closure) {
    selfType.map new ClosureFunction<T, V>(closure)
  }

  static <T> Promise<T> consume(final Promise<T> selfType, final Closure closure) {
    selfType.consume new ClosureConsumer<T>(closure)
  }

  static <T> Stream<T> filter(final Stream<T> selfType, final Closure<Boolean> closure) {
    selfType.filter new ClosurePredicate<T>(closure)
  }

  static <T, V> Stream<V> reduce(final Stream<T> selfType, final Closure<V> closure, V initial = null) {
//    selfType.reduce new ClosureReduce<T, V>(closure), initial
  }

  static <T, E> Stream<T> when(final Stream<T> selfType, final Class<E> exceptionType, final Closure closure) {
    selfType.when exceptionType, new ClosureConsumer<E>(closure)
  }

  static <T, E> Promise<T> when(final Promise<T> selfType, final Class<E> exceptionType, final Closure closure) {
    selfType.when exceptionType, new ClosureConsumer<E>(closure)
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

  static <T, V> Stream<V> mod(final Stream<T> selfType, final Function<Tuple2<T, V>, V> other) {
//    selfType.reduce other
  }

  static <T, V> Stream<V> mod(final Stream<T> selfType, final Closure<V> other) {
    reduce selfType, other
  }

  static <T, V> Stream<V> or(final Stream<T> selfType, final Function<T, V> other) {
    selfType.map other
  }

  static <T, V> Stream<V> or(final Stream<T> selfType, final Closure<V> other) {
    map selfType, other
  }

  static <T, V> Promise<V> or(final Promise<T> selfType, final Function<T, V> other) {
    selfType.then other, (Consumer<Throwable>) null
  }

  static <T, V> Promise<V> or(final Promise<T> selfType, final Closure<V> other) {
    then selfType, other
  }

  static <T, V> Stream<V> and(final Stream<T> selfType, final Predicate<T> other) {
    selfType.filter other
  }

  static <T> Stream<T> leftShift(final Stream<T> selfType, final Consumer<T> other) {
    selfType.consume other
  }

  static <T> Stream<T> leftShift(final Stream<T> selfType, final Closure other) {
    consume selfType, other
  }

  static <T> Promise<T> leftShift(final Promise<T> selfType, final Consumer<T> other) {
    selfType.onSuccess other
  }

  static <T> Promise<T> leftShift(final Promise<T> selfType, final Closure other) {
    onSuccess selfType, other
  }

  static <T> Stream<T> and(final Stream<T> selfType, final Closure<Boolean> other) {
    filter selfType, other
  }

  static <T> Consumer<T> leftShift(final Consumer<T> selfType, T value) {
    selfType.accept value
    selfType
  }

  static <T> Deferred<T, ?> leftShift(final Deferred<T, ?> selfType, T value) {
    selfType.accept value
  }

}
