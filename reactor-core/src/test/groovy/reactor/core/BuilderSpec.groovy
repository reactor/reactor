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

package reactor.core

import reactor.Fn
import reactor.fn.Consumer
import reactor.fn.Event
import spock.lang.Specification

import static reactor.Fn.$
/**
 * @author Stephane Maldini
 */
class BuilderSpec extends Specification {

	def "Reactor correctly built"() {

		given: "a plain Reactor"

		def reactor = Reactor.create().sync().get()
		def data = ""
		Thread t = null
		reactor.on($("test"), { ev ->
			data = ev.data
			t = Thread.currentThread()
		} as Consumer<Event<String>>)

		when:
		reactor.notify("test", Fn.event("Hello World!"))

		then:
		data == "Hello World!"
		Thread.currentThread() == t

	}


	def "Composable correctly built"() {

		given: "a plain Reactor"

		def reactor = Reactor.create().sync().get()
		def data = ""
		Thread t = null
		reactor.on($("test"), { ev ->
			data = ev.data
			t = Thread.currentThread()
		} as Consumer<Event<String>>)

		when:
		reactor.notify("test", Fn.event("Hello World!"))

		then:
		data == "Hello World!"
		Thread.currentThread() == t

	}


	def "Promise correctly built"() {

		when: "a plain Promise"
		def promise = Fn.compose('test').sync().get()

		then:
		promise.get() == 'test'

	}

}

