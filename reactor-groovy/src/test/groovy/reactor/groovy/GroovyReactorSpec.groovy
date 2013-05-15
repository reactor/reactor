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

import reactor.core.Context
import reactor.core.R
import reactor.fn.Event
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.Fn.$

/**
 * @author Stephane Maldini (smaldini)
 */
class GroovyReactorSpec extends Specification {

	def "Groovy Reactor dispatches events properly"() {

		given: "a simple reactor implementation"
		def r1 = R.create()
		def r2 = R.create()
		def latch = new CountDownLatch(1)

		when: 'Using simple arguments'
		def result = ""
		r1.on('test2') { String s ->
			result = s
			latch.countDown()
		}.
				notify 'test2', 'Hello'

		then:
		latch.await(5, TimeUnit.SECONDS)
		result == 'Hello'

		when: 'Using Selector and Consumer<Event> arguments'
		def data = ""
		def header = ""
		latch = new CountDownLatch(1)

		r2.on($('test')) { Event<String> s ->
			data = s.data
			header = s.headers['someHeader']
			latch.countDown()
		}
		r2.notify for: 'test', data: 'Hello World!', someHeader: 'test'

		then:
		latch.await(5, TimeUnit.SECONDS)
		data == "Hello World!"
		header == "test"

	}

	def "Groovy Reactor provides Closure as Supplier on notify"() {

		given: "a simple Reactor"
		def r = R.create(true)
		def result = ""
		r.on('supplier') { String s ->
			result = s
		}

		when: "a supplier is provided"
		r.notify('supplier', { "Hello World!" })

		then: "the result has been set"
		result == "Hello World!"

	}

	def "Groovy Reactor enables Actor programming style"() {

		given: "a simple reactor implementation"
		def reactor = R.create(true)

		when: 'Using simple arguments'
		def data2 = ""
		reactor.on({ String s ->
			data2 = s
		} as Closure) // ugly hack until I can get Groovy Closure invocation support built-in

		reactor << 'test2' << 'test3'

		then:
		data2 == 'test3'

	}

	def "Simple reactors linking"() {

		given: "normal reactors on the same thread"
		def r1 = R.create()
		def r2 = R.create()
		def r3 = R.create()
		def r4 = R.create()
		r1.dispatcher = r2.dispatcher = r3.dispatcher = r4.dispatcher = Context.synchronousDispatcher()

		def d1, d2, d3, d4

		//r1 | [r2, r3]
		r1 + r2 + r3
		// == r1 + r2 + r3
		//r1 | r2 | r3 has different meaning

		when: "registering few handlers"
		r1.on $('test'), { String test -> d1 = true }
		r2.on $('test'), { String test -> d2 = true }
		r3.on $('test'), { String test -> d3 = true }
		r4.on $('test'), { String test -> d4 = true }

		r1.notify 'test', new Event('bob')

		then: "r1,r2,r3 react"
		d1 && d2 && d3

		when: "registering a level 3 consumer"
		d1 = d2 = d3 = d4 = false

		r2 | r4
		r1.notify for: 'test', data: 'bob'

		then: "r1,r2,r3 and r4 react"
		d1 && d2 && d3 && d4

		when: "r2 is unlinked"
		d1 = d2 = d3 = d4 = false
		r1 - r2

		and: "sending on r1"
		r1.notify for: 'test', data: 'bob'

		then: "only r1,r3 react"
		d1 && d3 && !d2 && !d4

		when: "sending on r2"
		d1 = d2 = d3 = d4 = false
		r2.notify for: 'test', data: 'bob'

		then: "only r2,r4 react"
		d2 && d4 && !d1 && !d3

	}

	def "Reactor finder"() {

		when: "a simple reactor implementation"
		R.createOrGet('funkyReactor')

		then:
		'funkyReactor'.toReactor()

	}

}
