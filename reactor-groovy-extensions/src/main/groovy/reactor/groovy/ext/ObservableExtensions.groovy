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
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FirstParam
import reactor.bus.Bus
import reactor.bus.Event
import reactor.bus.EventBus
import reactor.bus.registry.Registration
import reactor.bus.selector.Selector
import reactor.bus.selector.Selectors
import reactor.fn.Consumer
import reactor.fn.Function
import reactor.fn.Supplier
import reactor.groovy.support.ClosureEventConsumer

import static Selectors.$
import static Selectors.object

/**
 * Extensions for providing syntax sugar for working with {@link Bus}s.
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
	static <T> Registration<Consumer<T>> react(EventBus selfType,
	                                        Selector selector,
	                                        @DelegatesTo(value = ClosureEventConsumer.ReplyDecorator,
			                                        strategy = Closure.DELEGATE_FIRST)
	                                        @ClosureParams(FirstParam.FirstGenericType)
			                                        Closure handler) {
		selfType.on selector, new ClosureEventConsumer<T>(handler)
	}

	static <T> Registration<Consumer<T>> react(EventBus selfType,
	                                        String selector,
	                                        @DelegatesTo(value = ClosureEventConsumer.ReplyDecorator,
			                                        strategy = Closure.DELEGATE_FIRST)
	                                        @ClosureParams(FirstParam.FirstGenericType)
			                                        Closure handler) {
		selfType.on object(selector), new ClosureEventConsumer<T>(handler)
	}

	/**
	 * Alias and Misc. Helpers
	 */
	static <T> EventBus send(EventBus selfType,
	                        Object key,
	                        T obj) {
		selfType.send key, Event.<T> wrap(obj)
	}

	static <T> EventBus send(EventBus selfType,
	                        Object key,
	                        T obj,
	                        @DelegatesTo(value = ClosureEventConsumer.ReplyDecorator, strategy = Closure.DELEGATE_FIRST) Closure handler) {
		send selfType, key, Event.wrap(obj), handler
	}

	static <T> EventBus send(EventBus selfType,
	                        Object key,
	                        Event<T> obj,
	                        @DelegatesTo(value = ClosureEventConsumer.ReplyDecorator, strategy = Closure.DELEGATE_FIRST) Closure handler) {
		def replyTo = obj.replyTo ? $(obj.replyTo) : Selectors.anonymous()
		selfType.on replyTo, new ClosureEventConsumer(handler)
		selfType.send key, obj.setReplyTo(replyTo.object)
	}

	static <T> Bus notify(Bus selfType,
	                                          Object key,
	                                          T obj) {
		selfType.notify key, Event.<T> wrap(obj)
	}

	static <T> Bus notify(Bus selfType,
	                                          Object key,
	                                          Supplier<Event<T>> obj) {
		selfType.notify key, obj.get()
	}

	static Bus notify(Bus selfType,
	                                      Object key) {
		selfType.notify key, Event.<Void> wrap(null)
	}

	static <T> Bus notify(Bus selfType,
	                                          String key,
	                                          Closure<T> closure) {
		selfType.notify key, Event.wrap((T) closure.call())
	}

	static Bus notify(final Bus selfType, final Map params) {
		Object topic = params.remove ARG_TOPIC

		def toSend
		if (params) {
			toSend = new Event(new Event.Headers(), params.remove(ARG_DATA))
			for (entry in params.entrySet()) {
				toSend.headers.set entry.key?.toString(), entry.value?.toString()
			}
		} else {
			toSend = new Event(params.remove(ARG_DATA))
		}

		selfType.notify topic, toSend
		selfType
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
