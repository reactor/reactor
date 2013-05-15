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

import reactor.core.Promise
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Stephane Maldini (smaldini)
 */
class GroovyComposableSpec extends Specification {

	def "Promise returns value"() {
		when: "a deferred Promise"
		def p = Promise.from("Hello World!").build()

		then: 'Promise contains value'
		p.get() == "Hello World!"
	}

	def "Promise notifies of Failure"() {
		when: "a deferred failed Promise"
		def p = Promise.from(new IllegalArgumentException("Bad code! Bad!")).build()

		and: "invoke result"
		p.get()

		then:
		p.state == Promise.State.FAILURE
		thrown(IllegalStateException)
	}

	def "Promises can be mapped"() {
		given: "a synchronous promise"
		def p = Promise.sync()

		when: "add a mapping closure"
		def s = p.map {
			Integer.parseInt it
		}

		and: "setting a value"
		p << '10'

		then:
		s.get() == 10
	}

	def "Promises can be filtered"() {
		given: "a synchronous promise"
		def p = Promise.sync()

		when: "add a mapping closure"
		def s = p.map {
			Integer.parseInt it
		}.filter {
			it > 10
		}

		and: "setting a value"
		p << '10'

		then: 'No value'
		!s.get()
	}

	def "A promise can be be consumed by another promise"() {
		given: "a synchronous promise"
		def p1 = Promise.sync()
		def p2 = Promise.sync()

		when: "p1 is consumed by p2"
		p1 / p2 //p1.consume p2

		and: "setting a value"
		p1 << 'Hello World!'

		then: 'P2 consumes the value from P1'
		p2.get() == 'Hello World!'
	}



	def "Errors stop compositions"() {
		given: "a synchronous promise"
		def p = Promise.create()
		final latch = new CountDownLatch(1)

		when: "p1 is consumed by p2"
		p.map { Integer.parseInt it }.
				when (NumberFormatException, { latch.countDown() }).
				filter { println('not in log'); true }

		and: "setting a value"
		p << 'not a number'

		then: 'No value'
		!p.await(500, TimeUnit.MILLISECONDS)
		latch.count == 0
	}

	def "Promise compose after set"() {
		given: "a synchronous promise"
		def p = Promise.sync('10')

		when: "p1 is consumed by p2"
		def s = p.map { Integer.parseInt it } map { it*10 }

		then:
		s.get() == 100
	}

}
