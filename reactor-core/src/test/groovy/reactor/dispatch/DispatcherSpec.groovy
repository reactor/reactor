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

package reactor.dispatch

import static reactor.GroovyTestUtils.$
import static reactor.GroovyTestUtils.consumer

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import reactor.Fn
import reactor.core.CachingRegistry
import reactor.fn.Consumer
import reactor.fn.Event
import reactor.fn.dispatch.SynchronousDispatcher
import reactor.fn.dispatch.ThreadPoolExecutorDispatcher
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class DispatcherSpec extends Specification {

	def "Dispatcher executes tasks in correct thread"() {

		given:
		def sameThread = new SynchronousDispatcher()
		def diffThread = new ThreadPoolExecutorDispatcher(1,128)
		def currentThread = Thread.currentThread()
		Thread taskThread = null
		def registry = new CachingRegistry<Consumer<Event>>(null, null)
		def sel = $('test')
		registry.register(sel, consumer {
			taskThread = Thread.currentThread()
		})

		when: "a task is submitted"
		def t = sameThread.nextTask()
		t.key = 'test'
		t.event = Fn.event("Hello World!")
		t.consumerRegistry = registry
		t.submit()

		then: "the task thread should be the current thread"
		currentThread == taskThread

		when: "a task is submitted to the thread pool dispatcher"
		def latch = new CountDownLatch(1)
		t = diffThread.nextTask()
		t.key = 'test'
		t.event = Fn.event("Hello World!")
		t.consumerRegistry = registry
		t.setCompletionConsumer({ Event<String> ev -> latch.countDown() } as Consumer<Event<String>>)
		t.submit()

		latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		then: "the task thread should be different when the current thread"
		taskThread != currentThread
		!diffThread.shutdown()

	}

}
