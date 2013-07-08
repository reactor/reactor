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

import reactor.R
import reactor.core.Environment

import static reactor.GroovyTestUtils.$
import static reactor.GroovyTestUtils.consumer

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import reactor.filter.PassThroughFilter
import reactor.function.Consumer
import reactor.event.Event
import reactor.event.dispatch.SynchronousDispatcher
import reactor.event.dispatch.ThreadPoolExecutorDispatcher
import reactor.event.registry.CachingRegistry
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringEventRouter;
import spock.lang.Specification

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class DispatcherSpec extends Specification {

	def "Dispatcher executes tasks in correct thread"() {

		given:
		def sameThread = new SynchronousDispatcher()
		def diffThread = new ThreadPoolExecutorDispatcher(1, 128)
		def currentThread = Thread.currentThread()
		Thread taskThread = null
		def registry = new CachingRegistry<Consumer<Event>>(null)
		def eventRouter = new ConsumerFilteringEventRouter(
				new PassThroughFilter(),  new ArgumentConvertingConsumerInvoker())
		def sel = $('test')
		registry.register(sel, consumer {
			taskThread = Thread.currentThread()
		})

		when: "a task is submitted"
		sameThread.dispatch('test', Event.wrap('Hello World!'), registry, null, eventRouter, null)

		then: "the task thread should be the current thread"
		currentThread == taskThread

		when: "a task is submitted to the thread pool dispatcher"
		def latch = new CountDownLatch(1)
		diffThread.dispatch('test', Event.wrap('Hello World!'), registry, null, eventRouter, { Event<String> ev -> latch.countDown() } as Consumer<Event<String>>)

		latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		then: "the task thread should be different when the current thread"
		taskThread != currentThread
		//!diffThread.shutdown()

	}

	def "Dispatcher thread can be reused"() {

		given: "ring buffer reactor"
		def r = R.reactor().env(new Environment()).dispatcher("ringBuffer").get()
		def latch = new CountDownLatch(2)

		when: "listen for recursive event"
		r.on(consumer { int i->
			if (i < 2) {
				latch.countDown()
				r.notify(Event.wrap(++i))
			}
		})

		and: "call the reactor"
		r.notify(Event.wrap(0))

		then: "a task is submitted to the thread pool dispatcher"
		latch.await(5, TimeUnit.SECONDS) // Wait for task to execute
	}

}
