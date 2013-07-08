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
import reactor.event.Event
import reactor.function.*
import reactor.event.registry.Registration
import reactor.event.selector.Selector
import reactor.groovy.support.ClosureEventConsumer
import reactor.groovy.support.ClosureEventFunction

import static reactor.Fn.$
/**
 * Extensions for providing syntax sugar for working with {@link reactor.function.Observable}s.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
@CompileStatic
class ObservableExtensions {
  static final String ARG_DATA = 'data'
  static final String ARG_TOPIC = 'for'

  /**
   * Closure converters
   */
  static <T, E extends Event<T>, V> Registration<Consumer<E>> receive(final Observable selfType,
                                                                      final Selector key,
                                                                      final Closure<V> closure) {
    selfType.receive key, new ClosureEventFunction<E, V>(closure)
  }

  static Registration<Consumer> on(Observable selfType,
                                   Selector selector,
                                   @DelegatesTo(value = ClosureEventConsumer, strategy = Closure.DELEGATE_FIRST) Closure handler) {
    selfType.on selector, new ClosureEventConsumer(handler)
  }

  static Registration<Consumer> on(Observable selfType,
                                   String selector,
                                   @DelegatesTo(value = ClosureEventConsumer, strategy = Closure.DELEGATE_FIRST) Closure handler) {
    selfType.on $(selector), new ClosureEventConsumer(handler)
  }

  static Registration<Consumer> on(Observable selfType,
                                   @DelegatesTo(value = ClosureEventConsumer, strategy = Closure.DELEGATE_FIRST) Closure handler) {
    selfType.on new ClosureEventConsumer(handler)
  }

  /**
   * Alias and Misc. Helpers
   */
  static <T> Observable notify(Observable selfType,
                               Object key,
                               T obj) {
    selfType.notify key, Event.<T> wrap(obj)
  }

  static <T> Observable notify(Observable selfType,
                               Object key,
                               Supplier<Event<T>> obj) {
    selfType.notify key, obj.get()
  }

  static Observable notify(Observable selfType,
                           Object key) {
    selfType.notify key, Event.<Void> wrap(null)
  }

  static <T> Observable notify(Observable selfType,
                               String key,
                               Closure<T> closure) {
    selfType.notify key, Event.wrap((T) closure.call())
  }

  static Observable notify(final Observable selfType, final Map<String, ?> params) {
    Object topic = params.remove ARG_TOPIC

    def toSend
    if (params) {
      toSend = new Event(new Event.Headers(), params.remove(ARG_DATA))
      for (entry in params.entrySet()) {
        toSend.headers.set entry.key, entry.value?.toString()
      }
    } else {
      toSend = new Event(params.remove(ARG_DATA))
    }

    selfType.notify topic, toSend
    selfType
  }

  /**
   * Operator overloading
   */

  static <T> Observable leftShift(final Observable selfType, final T obj) {
    selfType.notify Event.wrap(obj)
  }

  static <T> Observable leftShift(final Observable selfType, final Event<T> obj) {
    selfType.notify obj
  }

  static <T> Observable leftShift(final Observable selfType, final Closure<T> obj) {
    selfType.notify Event.wrap((T) obj.call())
  }

  static <T> Observable leftShift(final Observable selfType, final Supplier<Event<T>> obj) {
    selfType.notify obj.get()
  }

  /**
   * Alias
   */

  static <T, V> void call(final Function<T, V> selfType, final T value) {
    selfType.apply value
  }

  static <T> void call(final Consumer<T> selfType, final T value) {
    selfType.accept value
  }

  static <T> void call(final Supplier<T> selfType) {
    selfType.get()
  }

}
