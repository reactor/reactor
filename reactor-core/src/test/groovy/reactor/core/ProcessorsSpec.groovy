/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import reactor.core.processor.RingBufferProcessor
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class ProcessorsSpec extends Specification {

	@Shared
	ExecutorService executorService

	def setup() {
		executorService = Executors.newFixedThreadPool(2)
	}

	def cleanup() {
		executorService.shutdown()
	}

	private sub(String name, CountDownLatch latch) {
		new Subscriber<String>() {
			def s

			@Override
			void onSubscribe(Subscription s) {
				this.s = s
				println name + " " + Thread.currentThread().name + ": subscribe: " + s
				s.request(1)
			}

			@Override
			void onNext(String o) {
				println name + " " + Thread.currentThread().name + ": next: " + o
				latch.countDown()
				s.request(1)
			}

			@Override
			void onError(Throwable t) {
				println name + " " + Thread.currentThread().name + ": error:" + t
				t.printStackTrace()
			}

			@Override
			void onComplete() {
				println name + " " + Thread.currentThread().name + ": complete"
				//latch.countDown()
			}
		}
	}

	def "Dispatcher on Reactive Stream"() {

		given:
			"ring buffer processor with 16 backlog size"
			def bc = RingBufferProcessor.<String> create(executorService, 16)
			def bc2 = RingBufferProcessor.<String> create(executorService, 16)
			def elems = 18
			def latch = new CountDownLatch(elems)
			def manualSub = new Subscription(){
				@Override
				void request(long n) {
					println Thread.currentThread().name+" $n"
				}

				@Override
				void cancel() {
					println Thread.currentThread().name+" cancelling"
				}
			}

			bc.onSubscribe(manualSub)
			//bc.subscribe(bc2)
			//bc2.subscribe(sub('spec1', latch))
			bc.subscribe(sub('spec1', latch))

		when:
			"call the processor"
			elems.times {
				println "hello $it"
				bc.onNext 'hello ' + it
			}
			bc.onComplete()

			def ended = latch.await(50, TimeUnit.SECONDS) // Wait for task to execute

		then:
			"a task is submitted to the thread pool dispatcher"
			ended

		when:
			latch = new CountDownLatch(elems)
			bc = RingBufferProcessor.<String> create(executorService)
			bc.subscribe(sub('spec2', latch))

			elems.times {
				bc.onNext 'hello ' + it
			}
			bc.onComplete()

		then:
			"a task is submitted to the thread pool dispatcher"
			latch.await(50, TimeUnit.SECONDS) // Wait for task to execute
	}

}
