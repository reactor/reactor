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
import reactor.core.R
import reactor.fn.Event
import reactor.fn.Observable
import reactor.fn.Selector
import reactor.groovy.support.ClosureConsumer
import reactor.groovy.support.ClosureEventConsumer

/**
 * @author Jon Brisbin
 */
@CompileStatic
class RrrrrExtensions {

	/**
	 * Alias
	 */

	static <V> void call(final R selfType, final value, final Closure<V> closure) {
		schedule selfType, value, closure
	}

	static void notify(final R selfType, final Map<String, ?> params) {
		Object topic = params.remove ObservableExtensions.ARG_TOPIC

		def toSend
		if (params) {
			toSend = new Event(new Event.Headers(), params.remove(ObservableExtensions.ARG_DATA))
			for (entry in params.entrySet()) {
				toSend.headers.set entry.key, entry.value?.toString()
			}
		} else {
			toSend = new Event(params.remove(ObservableExtensions.ARG_DATA))
		}

		R.notify topic, toSend
	}

	/**
	 * Closure converters
	 */

	static <T> void schedule(final R selfType, final T value, final Closure closure) {
		R.schedule new ClosureConsumer<T>(closure), value
	}

	static <T> void schedule(final R selfType, final T value, final Observable observable, final Closure closure) {
		R.schedule new ClosureConsumer(closure), value, observable
	}

	static void on(final R selfType,
	               Selector selector,
	               @DelegatesTo(value = ClosureEventConsumer, strategy = Closure.DELEGATE_FIRST) Closure handler) {
		R.on selector, new ClosureEventConsumer(handler)
	}

	static void on(final R selfType,
	               final String id,
	               final Selector selector,
	               @DelegatesTo(value = ClosureEventConsumer, strategy = Closure.DELEGATE_FIRST) Closure handler) {
		R.on id, selector, new ClosureEventConsumer(handler)
	}

	/**
	 * Operator overloading
	 */

	static void or(final R selfType, Observable linkable) {
		R.link linkable
		R
	}
}
