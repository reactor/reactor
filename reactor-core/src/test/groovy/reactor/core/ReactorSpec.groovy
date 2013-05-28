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
import reactor.fn.Function
import spock.lang.Specification

import static reactor.Fn.$
import static reactor.Fn.R
import static reactor.GroovyTestUtils.consumer

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class ReactorSpec extends Specification {

	def "Reactor dispatches events properly"() {

		given: "a plain Reactor"
		def reactor = R.reactor().sync().get()
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

	def "Registrations are pausable and cancellable"() {

		given: "a simple reactor implementation"
		def reactor = R.reactor().sync().get()
		def data = ""
		def reg = reactor.on($("test"), { ev ->
			data = ev.data
		} as Consumer<Event<String>>)
		reg.pause()

		when: "event is triggered"
		reactor.notify("test", Fn.event("Hello World!"))

		then: "data should not have updated"
		data == ""

		when: "registration is cancelled"
		reg.cancel()
		reactor.notify("test", Fn.event("Hello World!"))

		then: "it shouldn't be found any more"
		data == ""
		!reactor.respondsToKey("test")

	}

	def "Reactors dispatch events based on a default selector"() {

		given: "a simple consumer"
		def reactor = R.reactor().sync().get()
		def data = ""
		def h = consumer { String s ->
			data = s
		}

		when: "a consumer is assigned based on a type selector"
		reactor.on h
		reactor.notify new Event<String>('Hello World!')

		then: "the data is updated"
		data == 'Hello World!'

	}

	def "Reactors respond to events"() {

		def r = R.reactor().sync().get()

		given: "a simple consumer"
		def data = ""
		def a = { ev ->
			data = ev.data
		}

		when: "an event is dispatched to the global reactor"
		r.on($('say-hello'), a as Consumer<Event<String>>)
		r.notify('say-hello', new Event<String>('Hello World!'))

		then: "the data is updated"
		data == 'Hello World!'

	}

	def "Reactors support send and receive"() {

		given: "a simple Reactor and a response-producing Function"
		def r = R.reactor().sync().get()
		def f = { s ->
			"Hello World!"
		} as Function<Event<String>, String>
		def result = ""
		def sel = $("hello")
		def replyTo = $()
		r.on(replyTo.t1, { ev ->
			result = ev.data
		} as Consumer<Event<String>>)

		when: "a Function is assigned that produces a Response and an Event is triggered"
		r.receive(sel, f)
		r.send("hello", Fn.event("Hello World!", replyTo.t2))

		then: "the result should have been updated"
		result == "Hello World!"

	}

	def "Consumers can be unassigned"() {

		given: "a normal reactor"
		def r = R.reactor().sync().get()

		when: "registering few handlers"
		r.on R('t[a-z]st'), consumer { println 'test1' }
		r.on R('t[a-z]st'), consumer { println 'test2' }

		r.notify "test", Fn.event("test")

		then: "will report false when asked whether it responds to an unmatched key"
		r.respondsToKey 'test'
		r.consumerRegistry.unregister('test')
		!r.respondsToKey('test')
	}

	def "Multiple consumers can use the same selector"() {

		given: "a normal synchronous reactor"
		def r = R.reactor().sync().get()
		def d1, d2
		def selector = $("test")

		when: "registering two consumers on the same selector"
		r.on(selector, consumer { d1 = true })
		r.on(selector, consumer { d2 = true })

		then: "both consumers are notified"
		r.notify 'test', new Event('foo')
		d1 && d2
	}

	def "Linking Reactors"() {

		given: "normal reactors on the same thread"
		def r1 = R.reactor().sync().get()
		def r2 = R.reactor().sync().get()
		def r3 = R.reactor().sync().get()
		def r4 = R.reactor().sync().get()

		def d1, d2, d3, d4

		r1.link(r2).link(r3)

		when: "registering few handlers"
		r1.on $('test'), Fn.consumer({ d1 = true })
		r2.on $('test'), Fn.consumer({ d2 = true })
		r3.on $('test'), Fn.consumer({ d3 = true })
		r4.on $('test'), Fn.consumer({ d4 = true })

		r1.notify 'test', new Event('bob')

		then: "r1,r2,r3 react"
		d1 && d2 && d3

		when: "registering a level 3 consumer"
		d1 = d2 = d3 = d4 = false

		r2.link r4
		r1.notify 'test', new Event('bob')

		then: "r1,r2,r3 and r4 react"
		d1 && d2 && d3 && d4

		when: "r2 is unlinked"
		d1 = d2 = d3 = d4 = false
		r1.unlink r2

		and: "sending on r1"
		r1.notify 'test', new Event('bob')

		then: "only r1,r3 react"
		d1 && d3 && !d2 && !d4

		when: "sending on r2"
		d1 = d2 = d3 = d4 = false
		r2.notify 'test', new Event('bob')

		then: "only r2,r4 react"
		d2 && d4 && !d1 && !d3

	}

}

