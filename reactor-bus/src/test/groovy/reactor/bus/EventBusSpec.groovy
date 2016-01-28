/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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

package reactor.bus

import groovy.transform.CompileStatic
import reactor.bus.filter.RoundRobinFilter
import reactor.bus.routing.ConsumerFilteringRouter
import reactor.bus.selector.Selectors
import reactor.fn.Consumer
import reactor.rx.Promise
import reactor.rx.Stream
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import static reactor.bus.GroovyTestUtils.*
import static reactor.bus.selector.Selectors.*

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class EventBusSpec extends Specification {

  def "A Reactor can be configured with EventBus.create()"() {

	when: "Building a Synchronous EventBus"
	def reactor = EventBus.config().sync().get()

	then: "Dispatcher has been set to Synchronous"
	reactor.processor instanceof Consumer

	when: "Building a RoundRobin EventBus"
	reactor = EventBus.config().roundRobinEventRouting().get()

	then: "EventRouter has been correctly set"
	reactor.router instanceof ConsumerFilteringRouter
	((ConsumerFilteringRouter) reactor.router).filter instanceof RoundRobinFilter
  }

  def "A Reactor can dispatch events properly"() {

	given: "a plain Reactor and a simple consumer on \$('test')"
	def reactor = EventBus.config().sync().get()
	def data = ""
	Thread t = null
	reactor.on($("test"), { ev ->
	  data = ev.data
	  t = Thread.currentThread()
	} as Consumer<Event<String>>)

	when: "Reactor is notified on 'test'"
	reactor.notify("test", Event.wrap("Hello World!"))

	then: "Data and T have been populated"
	data == "Hello World!"
	Thread.currentThread() == t

	when: "Reactor is notified on 'test' with a Supplier"
	data = ""
	reactor.notify("test", supplier { Event.wrap("Hello World!") })

	then: "Completion callback has been called"
	data == "Hello World!"

	when: "Reactor is notified without event"
	data = "something"
	reactor.notify('test')

	then: "Null event has been passed to the consumer"
	!data
  }

  class Foo {}

  class Bar {}

  @CompileStatic
  class TestConsumer implements Consumer<Event<Foo>> {

	final AtomicInteger value = new AtomicInteger()

	@Override
	void accept(Event<Foo> fooEvent) {
	  value.set(1)
	}
  }

  def "A Reactor fail fast if consumer of incorrect type"() {

	given: "a plain Reactor and a simple consumer on \$('test')"
	def reactor = EventBus.create()
	def c = new TestConsumer()
	reactor.on($("test"), c)

	when: "Reactor is notified on 'test'"
	reactor.notify("test", Event.wrap(new Bar()))

	then: "Data and T have been populated"
	c.value.get() == 0
  }

  def "A Registration is pausable and cancellable"() {

	given: "a simple eventBus implementation"
	def reactor = EventBus.config().sync().get()
	def data = ""
	def reg = reactor.on($("test"), { ev -> data = ev.data
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

  def "A Reactor can reply to events"() {

	def r = EventBus.config().sync().get()

	given: "a simple consumer"
	def data = ""
	def a = { ev -> data = ev.data
	}

	when: "an event is dispatched to the global eventBus"
	r.on($('say-hello'), a as Consumer<Event<String>>)
	r.notify('say-hello', new Event<String>('Hello World!'))

	then: "the data is updated"
	data == 'Hello World!'

  }

  def "A Reactor can support send and receive"() {

	given: "a simple Reactor and a response-producing Function"
	def r = EventBus.create()
	def f = function { s -> "Hello World!"
	}

	def sel = $("hello")
	def replyTo = $()

	def result = ""
	r.on(replyTo, { ev -> result = ev.data
	} as Consumer<Event<String>>)

	when: "a Function is assigned that produces a Response"
	r.receive(sel, f)

	and: "an Event is triggered"
	r.send("hello", Event.wrap("Hello World!", replyTo.object))

	then: "the result should have been updated"
	result == "Hello World!"

	when: "an event is triggered through Supplier#get()"
	result = ""
	r.send("hello", supplier { Event.wrap("Hello World!", replyTo.object) })

	then: "the result should have been updated"
	result == "Hello World!"

	when: "A different eventBus is provided"
	def r2 = EventBus.config().get()

	and: "A new consumer is attached"
	r2.on(replyTo, consumer {
	  result = it.data
	})

	and: "an event is triggered using the provided eventBus"
	result = ""
	r.send("hello", Event.wrap("Hello World!", replyTo.object), r2)

	then: "the result should have been updated"
	result == "Hello World!"

	when: "an event is triggered using the provided eventBus through Supplier#get()"
	result = ""
	r.send("hello", supplier { Event.wrap("Hello World!", replyTo.object) }, r2)

	then: "the result should have been updated"
	result == "Hello World!"

	when: "a registered function returns null"
	r.receive($('test3'), function { s -> null
	})

	and: "a replyTo consumer listens"
	result = 'something'
	r.on($('testReply3'), consumer { result = it.data })

	and: "send on 'test3'"
	r.send 'test3', Event.wrap('anything', 'testReply3')

	then: "result should be null"
	!result


	when: "a registered function rises error"
	r.receive($('test4'), function { s -> throw new Exception()
	})

	and: "a replyTo consumer listens"
	result = 'something'
	r.on($('testReply4'), consumer { result = it.data })

	and: "a T(Exception) consumer listens"
	def e
	r.on(T(Exception), consumer { e = it } as Consumer<Exception>)

	and: "send on 'test4'"
	r.send 'test4', Event.wrap('anything', 'testReply4')
	println r.debug()

	then: "result should not be null and error called"
	result
	e
  }

  def "A Consumer can be unassigned"() {

	given: "a normal eventBus"
	def reactor = EventBus.config().sync().get()

	when: "registering few handlers"
	reactor.on R('t[a-z]st'), { println 'test1' } as Consumer
	reactor.on R('t[a-z]st'), { println 'test2' } as Consumer

	reactor.notify "test", Event.wrap("test")

	then: "will report false when asked whether it responds to an unmatched key"
	reactor.respondsToKey 'test'
	reactor.consumerRegistry.unregister('test')
	!reactor.respondsToKey('test')

  }

  def "Multiple consumers can use the same selector"() {

	given: "a normal synchronous eventBus"
	def r = EventBus.config().sync().get()
	def d1, d2
	def selector = $("test")

	when: "registering two consumers on the same selector"
	r.on(selector, consumer { d1 = true })
	r.on(selector, consumer { d2 = true })

	then: "both consumers are notified"
	r.notify 'test', new Event('foo')
	d1 && d2
  }

  def "A Reactor can support single-use Consumers"() {

	given: "a synchronous Reactor and a single-use Consumer"
	def r = EventBus.config().get()
	def count = 0
	r.on($('test'), consumer { count++ }).cancelAfterUse()

	when: "the consumer is invoked several times"
	r.notify('test', Event.wrap(null))
	r.notify('test', Event.wrap(null))

	then: "the count is only 1"
	count == 1

  }

  def "A Reactor can register consumer for errors"() {

	given: "a synchronous Reactor"
	def r = EventBus.config().sync().get()

	when: "an error consumer is registered"
	def latch = new CountDownLatch(1)
	def e = null
	r.on(T(Exception),
			consumer { Exception ex -> e = ex; latch.countDown() }
					as Consumer<Exception>)

	and: "a normal consumer that rise exceptions"
	r.on($('test'), consumer { throw new Exception('bad') })

	and: "the consumer is invoked"
	r.notify('test', Event.wrap(null))

	then: "consumer has been invoked and e is an error"
	latch.await(3, TimeUnit.SECONDS)
	e && e instanceof Exception

  }

  def "A Reactor can run arbitrary consumer"() {

	given: "a synchronous Reactor"
	def r = EventBus.config().sync().get()

	when: "the Reactor default Selector is notified with a tuple of consumer and data"
	def latch = new CountDownLatch(1)
	def e = null
	r.schedule(consumer { e = it; latch.countDown() }, 'test')


	then: "consumer has been invoked and e is 'test'"
	latch.await(3, TimeUnit.SECONDS)
	e == 'test'

	when: "a consumer listen for failures"
	latch = new CountDownLatch(1)
	r.on(T(Exception),
			consumer { latch.countDown() }
					as Consumer<Exception>)

	and: "the arbitrary consumer fails"
	latch = new CountDownLatch(1)
	r.schedule(consumer { throw new Exception() }, 'test')


	then: "error consumer has been invoked"
	latch.await(3, TimeUnit.SECONDS)
  }

  def "A Reactor can handle errors"() {

	given: "a synchronous Reactor with a dispatch error handler"
	def count = 0
	def r = EventBus.config().
			sync().
			dispatchErrorHandler(consumer { t -> count++ }).
			uncaughtErrorHandler(consumer { t -> count++ }).
			get()
	r.on $('test'), consumer { ev -> throw new Exception("error") }

	when: "an error is produced"
	r.notify('test', Event.wrap(null))

	then: "the count has increased"
	count == 1

	when: "notify an error"
	r.notify('test', Event.wrap(new IllegalStateException("error")))

	then: "the count has increased"
	count == 2

  }

  def 'Creating Stream from event bus'() {
	given: 'a source stream with a given event bus'
	def r = EventBus.config().get()
	def selector = anonymous()
	int event = 0
	def s = r.on(selector).onBackpressureBuffer().map { it.data }.consume { event = it }
	println s.debug()

	when: 'accept a value'
	r.notify(selector.object, Event.wrap(1))
	println s.debug()

	then: 'dispatching works'
	event == 1

	when: "multithreaded bus can be serialized"
	r = EventBus.create(WorkQueueProcessor.create("bus", 8), 4)
	def tail = Stream.from(r.on(selector)).map { it.data }.doOnNext { sleep(100) }.elapsed().log().take(10).buffer()
			.promise()

	10.times {
	  r.notify(selector.object, Event.wrap(it))
	}

	println r.debug()

	then:
	tail.await().size() == 10
	tail.peek().sum { it.t1 } >= 1000 //correctly serialized

	cleanup:
	r.processor.onComplete()
  }

  def "An Observable can be used to consume a promise's value when it's fulfilled"() {
	given:
	"a promise with a consuming Observable"
	def promise = Promise.<Object> ready()
	def promise2 = Promise.<Object> ready()
	def bus = EventBus.create()

	bus.on(Selectors.$('key'), promise2)
	bus.notify(promise, 'key')

	when:
	"the promise is fulfilled"
	promise.onNext 'test'

	then:
	"the observable is notified"
	promise2.await()?.data == 'test'
  }

  def "An Observable can be used to consume the value of an already-fulfilled promise"() {
	given:
	"a fulfilled promise"
	def promise = Promise.success('test')
	def promise2 = Promise.<Object> ready()
	def bus = EventBus.create()

	when:
	"an Observable is added as a consumer"
	bus.on(Selectors.$('key'), promise2)
	bus.notify(promise, 'key')

	then:
	"the observable is notified"
	promise2.await()?.data == 'test'
  }
}

