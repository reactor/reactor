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


package reactor.groovy

import reactor.core.Environment
import reactor.rx.Promise
import reactor.rx.Promises
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Stephane Maldini
 */
class GroovyPromisesSpec extends Specification {

	@Shared
	Environment testEnv

	void setupSpec() {
		testEnv = new Environment()
	}

	def "Promise returns value"() {
		when: "a deferred Promise"
			def p = Promises.success("Hello World!")

		then: 'Promise contains value'
			p.get() == "Hello World!"
	}


	def "Promise from Closure"() {
		when: "a deferred Promise"
			def p = Promises.syncTask { "Hello World!" }

		then: 'Promise contains value'
			p.await() == "Hello World!"

		when: "a deferred Promise"
			p = Promise.<String> from { "Hello World!" }

		then: 'Promise contains value'
			p.await() == "Hello World!"
	}

	def "Compose from Closure"() {
		when:
			'Defer a composition'
			def c = Promises.<Integer>task(testEnv){ sleep 500; 1 }

		and:
			'apply a transformation'
			def d = c | { it + 1 }

		then:
			'Composition contains value'
			d.next().await() == 2
	}

	def "Promise notifies of Failure"() {
		when: "a deferred failed Promise"
			def p = Promises.error(new Exception("Bad code! Bad!"))

		and: "invoke result"
			p.get()

		then:
			p.error
			thrown(RuntimeException)

		when: "a deferred failed Promise with runtime exception"
			p = Promises.error(new IllegalArgumentException("Bad code! Bad!"))

		and: "invoke result"
			p.get()

		then:
			p.error
			thrown(IllegalArgumentException)
	}

	def "Promises can be mapped"() {
		given: "a synchronous promise"
			def p = Promises.<String> defer()

		when: "add a mapping closure"
			def s = p | { Integer.parseInt it }
		  def p2 = s.next()

		and: "setting a value"
			p << '10'

		then:
			p2.get() == 10

		when: "add a mapping closure"
			p = Promises.defer()
			s = p | { Integer.parseInt it }
		  p2 = s.next()

		and: "setting a value"
			p << '10'

		then:
			p2.get() == 10
	}

	def "A promise can be be consumed by another promise"() {
		given: "two synchronous promises"
			def p1 = Promises.<String> defer()
			def p2 = Promises.<String> defer()

		when: "p1 is consumed by p2"
			p1 << p2 //p1.consume p2

		and: "setting a value"
			p1 << 'Hello World!'

		then: 'P2 consumes the value when P1'
			p2.get() == 'Hello World!'
	}


	def "Errors stop compositions"() {
		given: "a promise"
			def p = Promises.<String> defer(testEnv)
			final latch = new CountDownLatch(1)

		when: "p1 is consumed by p2"
			def s = p.stream().map() { Integer.parseInt it }.
					when(NumberFormatException, { latch.countDown(); println 'test'; }).
					map { println('not in log'); true }

		and: "setting a value"
			p << 'not a number'
			s.next().await(2000, TimeUnit.MILLISECONDS)

		then: 'No value'
			thrown(NumberFormatException)
			latch.await(2, TimeUnit.SECONDS) && latch.count == 0
	}

	def "Promise compose after set"() {
		given: "a synchronous promise"
			def p = Promises.success('10')

		when: "composing 2 functions"
			def s = p | { Integer.parseInt it } | { it * 10 }

		then:
			s.next().get() == 100
	}

}
