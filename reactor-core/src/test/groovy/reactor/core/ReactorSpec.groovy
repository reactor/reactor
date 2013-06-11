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
package reactor.core

import reactor.Fn
import reactor.R
import reactor.filter.RoundRobinFilter
import reactor.fn.Consumer
import reactor.fn.Event
import reactor.fn.Function
import reactor.fn.dispatch.SynchronousDispatcher
import reactor.fn.routing.ConsumerFilteringEventRouter
import reactor.fn.support.SingleUseConsumer
import reactor.fn.tuples.Tuple2
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.Fn.$
import static reactor.Fn.R
import static reactor.GroovyTestUtils.consumer
import static reactor.GroovyTestUtils.supplier
import static reactor.GroovyTestUtils.function

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class ReactorSpec extends Specification {

	@Shared
	Environment testEnv

	void setupSpec() {
		testEnv = new Environment()
	}

	def "A Reactor can be configured with R.reactor()"() {

		when: "Building a Synchronous Reactor"
		def reactor = R.reactor().sync().get()

		then: "Dispatcher has been set to Synchronous"
		reactor.dispatcher == SynchronousDispatcher.INSTANCE

		when: "Building a RoundRobin Reactor"
		reactor = R.reactor().roundRobinEventRouting().get()

		then: "EventRouter has been correctly set"
		reactor.eventRouter instanceof ConsumerFilteringEventRouter
		((ConsumerFilteringEventRouter) reactor.eventRouter).filter instanceof RoundRobinFilter
	}


	def "A Reactor can be registered with an environment"() {

		given: "an environment-aware registered reactor"
		def r1 = R.reactor().using(testEnv).register().get()

		when: "a reactor is found through its internal id"
		def r2 = testEnv.find(r1.id)

		then: "both reactors match"
		r1 == r2
		r1.equals r2

		when: "a reactor is found through an arbitrary id"
		r1 = R.reactor().using(testEnv).register('testReactor').get()
		r2 = testEnv.find('testReactor')

		then: "both reactors match"
		r1 == r2
		r1.equals r2
	}

	def "A Reactor can dispatch events properly"() {

		given: "a plain Reactor and a simple consumer on \$('test')"
		def reactor = R.reactor().sync().get()
		def data = ""
		Thread t = null
		reactor.on($("test"), { ev ->
			data = ev.data
			t = Thread.currentThread()
		} as Consumer<Event<String>>
		)

		when: "Reactor is notified on 'test'"
		reactor.notify("test", Event.wrap("Hello World!"))

		then: "Data and T have been populated"
		data == "Hello World!"
		Thread.currentThread() == t

		when: "Reactor is notified on 'test' and completion callback is attached"
		def completion = ""
		reactor.notify("test", Event.wrap("Hello World!"), consumer { completion = it.data })

		then: "Completion callback has been called"
		completion == "Hello World!"

		when: "Reactor is notified on 'test' with a Supplier"
		data = ""
		reactor.notify("test", supplier { Event.wrap("Hello World!") })

		then: "Completion callback has been called"
		data == "Hello World!"

		when: "Reactor listens on default Selector"
		data = ""
		reactor.on(consumer { data = it.data })

		and: "Reactor is notified on default Selector"
		reactor.notify(Event.wrap("Hello World!"))

		then: "Default selector has been called"
		data == "Hello World!"

		when: "Reactor is notified on default Selector with a Supplier"
		reactor.notify(supplier { Event.wrap("Hello World!") })

		then: "Default selector has been called"
		data == "Hello World!"

		when: "Reactor is notified without event"
		data = "something"
		reactor.notify('test')

		then: "Null event has been passed to the consumer"
		!data
	}

	def "A Registration is pausable and cancellable"() {

		given: "a simple reactor implementation"
		def reactor = R.reactor().sync().get()
		def data = ""
		def reg = reactor.on($("test"), { ev ->
			data = ev.data
		} as Consumer<Event<String>>)
		reg.pause()

		when: "event is triggered"
		reactor.notify("test", Event.wrap("Hello World!"))

		then: "data should not have updated"
		data == ""

		when: "registration is cancelled"
		reg.cancel()
		reactor.notify("test", Event.wrap("Hello World!"))

		then: "it shouldn't be found any more"
		data == ""
		!reactor.respondsToKey("test")

	}

	def "A Reactor can dispatch events based on a default selector"() {

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

	def "A Reactor can reply to events"() {

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

	def "A Reactor can support send and receive"() {

		given: "a simple Reactor and a response-producing Function"
		def r = R.reactor().sync().get()
		def f = function { s ->
			"Hello World!"
		}

		def sel = $("hello")
		def replyTo = $()

		def result = ""
		r.on(replyTo.t1, { ev ->
			result = ev.data
		} as Consumer<Event<String>>)

		when: "a Function is assigned that produces a Response"
		r.receive(sel, f)

		and: "an Event is triggered"
		r.send("hello", Event.wrap("Hello World!", replyTo.t2))

		then: "the result should have been updated"
		result == "Hello World!"

		when: "an event is triggered through Supplier#get()"
		result = ""
		r.send("hello", supplier { Event.wrap("Hello World!", replyTo.t2) })

		then: "the result should have been updated"
		result == "Hello World!"

		when: "A different reactor is provided"
		def r2 = R.reactor().get()

		and: "A new consumer is attached"
		r2.on(replyTo.t1, consumer {
			result = it.data
		})

		and: "an event is triggered using the provided reactor"
		result = ""
		r.send("hello", Event.wrap("Hello World!", replyTo.t2), r2)

		then: "the result should have been updated"
		result == "Hello World!"

		when: "an event is triggered using the provided reactor through Supplier#get()"
		result = ""
		r.send("hello", supplier { Event.wrap("Hello World!", replyTo.t2) }, r2)

		then: "the result should have been updated"
		result == "Hello World!"

		when: "a registered function returns null"
		r.receive($('test3'), function { s ->
			null
		})

		and: "a replyTo consumer listens"
		result = 'something'
		r.on($('testReply3'), consumer {result = it.data})

		and: "send on 'test3'"
		r.send 'test3', Event.wrap('anything', 'testReply3')

		then: "result should be null"
		!result


		when: "a registered function rises exception"
		r.receive($('test4'), function { s ->
			throw new Exception()
		})

		and: "a replyTo consumer listens"
		result = 'something'
		r.on($('testReply4'), consumer {result = it.data})

		and: "a T(Exception) consumer listens"
		def e
		r.on(Fn.T(Exception), consumer {e = it} as Consumer<Exception>)

		and: "send on 'test4'"
		r.send 'test4', Event.wrap('anything', 'testReply4')

		then: "result should not be null and exception called"
		result
		e
	}

	def "A Reactor can map replies to Stream"() {
		given: "a synchronous Reactor"
		def r = R.reactor().sync().get()

		when: "mapping a default Function"
		def s = r.map function { Integer.parseInt it.data }

		and: "Notify reactor default selector"
		r.notify Event.wrap('1')

		then: "Stream result should be set"
		s.get() == 1

		when: "mapping a 'test' Function"
		s = r.map $('test'), function { Integer.parseInt it.data }

		and: "Notify reactor 'test' selector"
		r.notify 'test', Event.wrap('1')

		then: "Stream result should be set"
		s.get() == 1

		when: "mapping a 'test2' Function"
		s = r.map $('test2'), function { throw new Exception() }
		def ex
		s.when(Exception, consumer { ex = it })

		and: "Notify reactor 'test2' selector"
		r.notify 'test2', Event.wrap('1')

		then: "Exception should be set"
		ex
	}

	def "A Reactor can compose replies to Stream"() {
		given: "a synchronous Reactor"
		def r = R.reactor().sync().get()

		when: "mapping a 'test' Function"
		r.receive $('test'), function { Integer.parseInt it.data }

		and: "Notify reactor 'test' selector"
		def s = r.compose 'test', Event.wrap('1')

		then: "Stream result should be set"
		s.get() == 1

		when: "Notify reactor 'test' selector"
		s = r.compose 'test', supplier{Event.wrap('1')}

		then: "Stream result should be set"
		s.get() == 1

		when: "mapping a new 'test' Function"
		r.receive $('test'), function { Integer.parseInt(it.data) * 10 }

		and: "Notify reactor 'test' selector (2 previous functions registered)"
		s = r.compose 'test', Event.wrap('1')

		then: "Stream result should be set"
		s.reduce().get() == [1, 10]

		when: "Notify reactor 'test' selector with a provided consumer"
		def count = 0
		r.compose 'test', Event.wrap('1'), consumer { count++ }

		then: "The consumer has received 2 results"
		count == 2

		when: "Notify reactor 'test' selector with supplier and a provided consumer"
		count = 0
		r.compose 'test', supplier { Event.wrap('1') }, consumer { count++ }

		then: "The consumer has received 2 results"
		count == 2
	}


	def "A Consumer can be unassigned"() {

		given: "a normal reactor"
		def r = R.reactor().sync().get()

		when: "registering few handlers"
		r.on R('t[a-z]st'), consumer { println 'test1' }
		r.on R('t[a-z]st'), consumer { println 'test2' }

		r.notify "test", Event.wrap("test")

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

	def "A Reactor can be linked to another Reactor"() {

		given: "normal reactors on the same thread"
		def r1 = R.reactor().sync().get()
		def r2 = R.reactor().using(r1).link().get()
		def r3 = R.reactor().using(r1).link().get()
		def r4 = R.reactor().sync().get()

		def d1, d2, d3, d4


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

	def "A Reactor can support single-use Consumers"() {

		given: "a synchronous Reactor and a single-use Consumer"
		def r = R.reactor().get()
		def count = 0
		r.on(new SingleUseConsumer(consumer { count++ }))

		when: "the consumer is invoked several times"
		r.notify(Event.wrap(null))
		r.notify(Event.wrap(null))

		then: "the count is only 1"
		count == 1

	}


	def "A Reactor can receive registration notifications"() {

		given: "a synchronous Reactor and a registration consumer"
		def r = R.reactor().get()
		def count = 0
		r.onRegistration(consumer { count++ })

		when: "a consumer is registered"
		r.on(consumer {})

		then: "the count should be 1 as we had a single registration"
		count == 1

		when: "a synchronous RoundRobin Reactor and 2 registration consumers"
		r = R.reactor().roundRobinEventRouting().get()
		count = 0
		r.onRegistration(consumer { count++ })
		r.onRegistration(consumer { count++ })

		and: "register a consumer"
		r.on(consumer {})

		then: "the count should be 2 as we had 2 registrations consumers called"
		count == 2

	}

	def "Environment provides a root reactor"() {

		given: "a root Reactor from environment and a simple Consumer"
		def r = testEnv.rootReactor
		def latch = new CountDownLatch(1)
		r.on(consumer { latch.countDown() })

		when: "the consumer is invoked"
		r.notify(Event.wrap(null))

		then: "the consumer has been triggered"
		latch.await(3, TimeUnit.SECONDS)

	}

	def "A Reactor can register consumer for errors"() {

		given: "a synchronous Reactor"
		def r = R.reactor().sync().get()

		when: "an error consumer is registered"
		def latch = new CountDownLatch(1)
		def e = null
		r.on(Fn.T(Exception),
				consumer { Exception ex -> e = ex; latch.countDown() }
				as Consumer<Exception>
		)

		and: "a normal consumer that rise exceptions"
		r.on(consumer { throw new Exception('bad') })

		and: "the consumer is invoked"
		r.notify(Event.wrap(null))

		then: "consumer has been invoked and e is an exception"
		latch.await(3, TimeUnit.SECONDS)
		e && e instanceof Exception

	}


	def "A Reactor can run arbitrary consumer"() {

		given: "a synchronous Reactor"
		def r = R.reactor().sync().get()

		when: "the Reactor default Selector is notified with a tuple of consumer and data"
		def latch = new CountDownLatch(1)
		def e = null
		r.notify(Event.wrap(Tuple2.of(consumer { e = it; latch.countDown() }, 'test')))


		then: "consumer has been invoked and e is 'test'"
		latch.await(3, TimeUnit.SECONDS)
		e == 'test'

		when: "a consumer listen for failures"
		latch = new CountDownLatch(1)
		r.on(Fn.T(Exception),
				consumer { latch.countDown() }
				as Consumer<Exception>)

		and: "the arbitrary consumer fails"
		latch = new CountDownLatch(1)
		r.notify(Event.wrap(Tuple2.of(consumer { throw new Exception() }, 'test')))


		then: "error consumer has been invoked"
		latch.await(3, TimeUnit.SECONDS)
	}


}

