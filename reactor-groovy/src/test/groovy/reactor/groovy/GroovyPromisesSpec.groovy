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

package reactor.groovy

import reactor.core.Environment
import reactor.core.Promise
import reactor.P
import reactor.fn.dispatch.BlockingQueueDispatcher
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
/**
 * @author Stephane Maldini
 */
class GroovyPromisesSpec extends Specification {

	@Shared def testEnv

	void setupSpec(){
		testEnv = new Environment()
		testEnv.addDispatcher('eventLoop',new BlockingQueueDispatcher('eventLoop', 256))
	}

	def "Promise returns value"() {
		when: "a deferred Promise"
		def p = P.success("Hello World!").get()

		then: 'Promise contains value'
		p.get() == "Hello World!"
	}


	def "Promise from Closure"() {
		when: "a deferred Promise"
		def p = P.task{"Hello World!"}.get()

		then: 'Promise contains value'
		p.await() == "Hello World!"

		when: "a deferred Promise"
		p = Promise.from{"Hello World!"}.get()

		then: 'Promise contains value'
		p.await() == "Hello World!"
	}

	def "Promise notifies of Failure"() {
		when: "a deferred failed Promise"
		def p = P.error(new IllegalArgumentException("Bad code! Bad!")).get()

		and: "invoke result"
		p.get()

		then:
		p.error
		thrown(IllegalStateException)
	}

	def "Promises can be mapped"() {
		given: "a synchronous promise"
		def p = P.defer().get()

		when: "add a mapping closure"
		def s = p | { Integer.parseInt it }

		and: "setting a value"
		p << '10'

		then:
		s.get() == 10

		when: "add a mapping closure"
		p = P.defer().get()
		s = p.then { Integer.parseInt it }

		and: "setting a value"
		p << '10'

		then:
		s.get() == 10
	}

	def "Promises can be filtered"() {
		given: "a synchronous promise"
		def p = P.defer().get()

		when: "add a mapping closure and a filter"
		def s = (p | { Integer.parseInt it }) & { it > 10 }

		and: "setting a value"
		p << '10'

		then: 'No value'
		!s.get()
	}

	def "A promise can be be consumed by another promise"() {
		given: "two synchronous promises"
		def p1 = P.defer().get()
		def p2 = P.defer().get()

		when: "p1 is consumed by p2"
		p1 << p2 //p1.consume p2

		and: "setting a value"
		p1 << 'Hello World!'

		then: 'P2 consumes the value when P1'
		p2.get() == 'Hello World!'
	}



	def "Errors stop compositions"() {
		given: "a promise"
		def p = P.defer().using(testEnv).eventLoop().get()
		final latch = new CountDownLatch(1)

		when: "p1 is consumed by p2"
		def s = p.map{ Integer.parseInt it }.
				when (NumberFormatException, { latch.countDown() }).
				filter{ println('not in log'); true }

		and: "setting a value"
		p << 'not a number'
		s.await(500, TimeUnit.MILLISECONDS)

		then: 'No value'
		thrown(IllegalStateException)
		latch.count == 0
	}

	def "Promise compose after set"() {
		given: "a synchronous promise"
		def p = P.success('10').get()

		when: "composing 2 functions"
		def s = p | { Integer.parseInt it } | { it*10 }

		then:
		s.get() == 100
	}

}
