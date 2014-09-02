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


package reactor.dispatch

import com.lmax.disruptor.BlockingWaitStrategy
import com.lmax.disruptor.dsl.ProducerType
import reactor.core.Environment
import reactor.core.Reactor
import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.event.dispatch.RingBufferDispatcher
import reactor.event.dispatch.SynchronousDispatcher
import reactor.event.dispatch.ThreadPoolExecutorDispatcher
import reactor.event.dispatch.WorkQueueDispatcher
import reactor.event.registry.CachingRegistry
import reactor.event.routing.ArgumentConvertingConsumerInvoker
import reactor.event.routing.ConsumerFilteringRouter
import reactor.filter.PassThroughFilter
import reactor.function.Consumer
import reactor.rx.spec.Streams
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.GroovyTestUtils.$
import static reactor.event.selector.Selectors.T

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class DispatcherSpec extends Specification {

	@Shared
	Environment env

	void setup() {
		env = new Environment()
	}

	def cleanup(){
		env.shutdown()
	}

	def "Dispatcher executes tasks in correct thread"() {

		given:
			def sameThread = new SynchronousDispatcher()
			def diffThread = new ThreadPoolExecutorDispatcher(1, 128)
			def currentThread = Thread.currentThread()
			Thread taskThread = null
			def registry = new CachingRegistry<Consumer<Event>>()
			def eventRouter = new ConsumerFilteringRouter(
					new PassThroughFilter(), new ArgumentConvertingConsumerInvoker())
			def sel = $('test')
			registry.register(sel, { Event<?> ev ->
				taskThread = Thread.currentThread()
			} as Consumer<Event<?>>)

		when:
			"a task is submitted"
			sameThread.dispatch('test', Event.wrap('Hello World!'), registry, null, eventRouter, null)

		then:
			"the task thread should be the current thread"
			currentThread == taskThread

		when:
			"a task is submitted to the thread pool dispatcher"
			def latch = new CountDownLatch(1)
			diffThread.dispatch('test', Event.wrap('Hello World!'), registry, null, eventRouter, { Event<String> ev -> latch.countDown() } as Consumer<Event<String>>)

			latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		then:
			"the task thread should be different when the current thread"
			taskThread != currentThread
			//!diffThread.shutdown()

	}

	def "Dispatcher thread can be reused"() {

		given:
			"ring buffer reactor"
			def r = Reactors.reactor().env(env).dispatcher("ringBuffer").get()
			def latch = new CountDownLatch(2)

		when:
			"listen for recursive event"
			r.on($('test')) { Event<Integer> ev ->
				if (ev.data < 2) {
					latch.countDown()
					r.notify('test', Event.wrap(++ev.data))
				}
			}

		and:
			"call the reactor"
			r.notify('test', Event.wrap(0))

		then:
			"a task is submitted to the thread pool dispatcher"
			latch.await(5, TimeUnit.SECONDS) // Wait for task to execute
	}

	def "Dispatchers can be shutdown awaiting tasks to complete"() {

		given:
			"a Reactor with a ThreadPoolExecutorDispatcher"
			def r = Reactors.reactor().
					env(env).
					dispatcher(Environment.THREAD_POOL).
					get()
			long start = System.currentTimeMillis()
			def hello = ""
			r.on($("pause"), { Event<String> ev ->
				hello = ev.data
				Thread.sleep(1000)
			} as Consumer<Event<?>>)

		when:
			"the Dispatcher is shutdown and tasks are awaited"
			r.notify("pause", Event.wrap("Hello World!"))
			def success = r.dispatcher.awaitAndShutdown(5, TimeUnit.SECONDS)
			long end = System.currentTimeMillis()

		then:
			"the Consumer was run, this thread was blocked, and the Dispatcher is shut down"
			hello == "Hello World!"
			success
			(end - start) >= 1000

	}

	def "RingBufferDispatcher doesn't deadlock on thrown Exception"() {

		given:
			def dispatcher = new RingBufferDispatcher("rb", 8, null, ProducerType.MULTI, new BlockingWaitStrategy())
			def r = new Reactor(dispatcher)

		when:
			def stream = Streams.<Throwable> defer()
			def promise = stream.limit(16).count().toList()
			r.on(T(Throwable), stream.toBroadcastNextConsumer())
			r.on($("test"), { ev ->
				sleep(100)
				1 / 0
			} as Consumer<Event<String>>)
			16.times {
				r.notify "test", Event.wrap("test")
			}
			println stream.debug()

		then:
			promise.await(5, TimeUnit.SECONDS)
			promise.get() == [16]

	}

	def "RingBufferDispatcher executes tasks in correct thread"() {

		given:
			def dispatcher = new RingBufferDispatcher("rb", 8, null, ProducerType.MULTI, new BlockingWaitStrategy())
			def t1 = Thread.currentThread()
			def t2 = Thread.currentThread()

		when:
			dispatcher.execute({ t2 = Thread.currentThread() })
			Thread.sleep(500)

		then:
			t1 != t2

	}

	def "WorkQueueDispatcher executes tasks in correct thread"() {

		given:
			def dispatcher = new WorkQueueDispatcher("rb", 8, 1024, null, ProducerType.MULTI, new BlockingWaitStrategy())
			def t1 = Thread.currentThread()
			def t2 = Thread.currentThread()

		when:
			dispatcher.execute({ t2 = Thread.currentThread() })
			Thread.sleep(500)

		then:
			t1 != t2

	}

}
