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
import reactor.Fn
import reactor.core.Composable
import reactor.core.Reactor
import reactor.fn.*
import reactor.groovy.support.ClosureConsumer
import reactor.groovy.support.ClosureEventConsumer
import reactor.groovy.support.ClosureEventFunction

import static reactor.Fn.$
import static reactor.core.R.get

/**
 * Extensions for providing syntax suger for working with {@link reactor.fn.Observable}s.
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

	static <V> Composable<V> map(Reactor selfType,
	                             final String key,
	                             @DelegatesTo(value = ClosureEventFunction, strategy = Closure.DELEGATE_FIRST)
	                             Closure<V> handler) {
		selfType.map $(key), new ClosureEventFunction<?, V>(handler)
	}

	static <V> Composable<V> map(Reactor selfType,
	                             final Selector key,
	                             @DelegatesTo(value = ClosureEventFunction, strategy = Closure.DELEGATE_FIRST)
	                             Closure<V> handler) {
		selfType.map key, new ClosureEventFunction<?, V>(handler)
	}

	static <V> Composable<V> map(Reactor selfType,
	                             @DelegatesTo(value = ClosureEventFunction, strategy = Closure.DELEGATE_FIRST)
	                             Closure<V> handler) {
		selfType.map new ClosureEventFunction(handler)
	}

	static <T> Reactor compose(Reactor selfType,
	                           Object key,
	                           T obj,
	                           @DelegatesTo(value = ClosureEventFunction, strategy = Closure.DELEGATE_FIRST)
	                           Closure handler) {
		selfType.compose key, Fn.event(obj), new ClosureConsumer<T>(handler)
	}

	static <T> Reactor compose(Reactor selfType,
	                           Object key,
	                           Event<T> obj,
	                           @DelegatesTo(value = ClosureEventFunction, strategy = Closure.DELEGATE_FIRST)
	                           Closure handler) {
		selfType.compose key, obj, new ClosureConsumer<T>(handler)
	}

	/**
	 * Alias and Misc. Helpers
	 */

	static <T, V> Composable<V> compose(Reactor selfType,
	                                    Object key,
	                                    T obj) {
		selfType.compose key, Fn.event(obj)
	}


	static <T> Reactor compose(Reactor selfType,
	                                    Object key,
	                                    T obj,
	                                    Consumer<T> consumer) {
		selfType.compose key, Fn.event(obj), consumer
	}


	static <T> Observable notify(Observable selfType,
	                             Object key,
	                             T obj) {
		selfType.notify key, Fn.<T> event(obj)
	}

	static <T> Observable notify(Observable selfType,
	                             Object key,
	                             Supplier<Event<T>> obj) {
		selfType.notify key, obj.get()
	}

	static Observable notify(Observable selfType,
	                         Object key) {
		selfType.notify key, Fn.<Void> event(null)
	}

	static <T> Observable notify(Observable selfType,
	                             String key,
	                             Closure<T> closure) {
		selfType.notify key, Fn.event((T) closure.call())
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
		selfType.notify Fn.event(obj)
	}

	static <T> Observable leftShift(final Observable selfType, final Event<T> obj) {
		selfType.notify obj
	}

	static <T> Observable leftShift(final Observable selfType, final Closure<T> obj) {
		selfType.notify Fn.event((T) obj.call())
	}

	static <T> Observable leftShift(final Observable selfType, final Supplier<Event<T>> obj) {
		selfType.notify obj.get()
	}

}
