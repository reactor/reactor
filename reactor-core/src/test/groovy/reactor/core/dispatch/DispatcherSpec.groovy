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


package reactor.core.dispatch

import reactor.Environment
import reactor.core.Dispatcher
import reactor.fn.Consumer
import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class DispatcherSpec extends Specification {

	@Shared
	Environment env

	def setup() {
		env = new Environment().assignErrorJournal()
	}

	def cleanup() {
		env.shutdown()
	}

	def "Dispatcher executes tasks in correct thread"() {

		given:
			def sameThread = new SynchronousDispatcher()
			def diffThread = new ThreadPoolExecutorDispatcher(1, 128)
			def currentThread = Thread.currentThread()
			Thread taskThread = null
			def consumer = { ev ->
				taskThread = Thread.currentThread()
			}

		when:
			"a task is submitted"
			sameThread.dispatch('test', consumer, null)

		then:
			"the task thread should be the current thread"
			currentThread == taskThread

		when:
			"a task is submitted to the thread pool dispatcher"
			def latch = new CountDownLatch(1)
			diffThread.dispatch('test', { ev -> consumer(ev); latch.countDown() }, null)

			latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		then:
			"the task thread should be different when the current thread"
			taskThread != currentThread
			//!diffThread.shutdown()

	}

	def "Dispatcher thread can be reused"() {

		given:
			"ring buffer eventBus"
			def r = env.getDispatcher(Environment.SHARED)
			def latch = new CountDownLatch(2)

		when:
			"listen for recursive event"
		  Consumer<Integer> c
			c = { data ->
				if(data < 2) {
					latch.countDown()
					r.dispatch(++data, c, null)
				}
			}

		and:
			"call the eventBus"
			r.dispatch(0, c, null)

		then:
			"a task is submitted to the thread pool dispatcher"
			latch.await(5, TimeUnit.SECONDS) // Wait for task to execute
	}

	def "Dispatchers can be shutdown awaiting tasks to complete"() {

		given:
			"a Reactor with a ThreadPoolExecutorDispatcher"
			def r  = env.getDispatcher(Environment.THREAD_POOL)
			long start = System.currentTimeMillis()
			def hello = ""
			def c = { String ev ->
				hello = ev
				Thread.sleep(1000)
			} as Consumer<String>

		when:
			"the Dispatcher is shutdown and tasks are awaited"
			r.dispatch("Hello World!", c, null)
			def success = r.awaitAndShutdown(5, TimeUnit.SECONDS)
			long end = System.currentTimeMillis()

		then:
			"the Consumer was run, this thread was blocked, and the Dispatcher is shut down"
			hello == "Hello World!"
			success
			(end - start) >= 1000

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

	def "MultiThreadDispatchers support ping pong dispatching"(Dispatcher d) {
		given:
			def latch = new CountDownLatch(4)
			def main = Thread.currentThread()
			def t1 = Thread.currentThread()
			def t2 = Thread.currentThread()

		when:
			Consumer<String> pong

			def ping = {
				if (latch.count > 0) {
					t1 = Thread.currentThread()
					d.dispatch("pong", pong, null)
					latch.countDown()
				}
			}
			pong = {
				if (latch.count > 0) {
					t2 = Thread.currentThread()
					d.dispatch("ping", ping, null)
					latch.countDown()
				}
			}

			d.dispatch("ping", ping, null)

		then:
			latch.await(1, TimeUnit.SECONDS)
			main != t1
			main != t2
			t1 != t2

		where:
			d << [
					new ThreadPoolExecutorDispatcher(4, 1024),
					new WorkQueueDispatcher("ping-pong", 4, 1024, null)
			]

	}

}
