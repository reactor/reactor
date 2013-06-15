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

import reactor.S
import reactor.core.Environment
import reactor.core.Promises
import reactor.R
import reactor.fn.dispatch.BlockingQueueDispatcher
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.Fn.$
/**
 * @author Stephane Maldini
 */
class GroovyStreamSpec extends Specification {

	@Shared def testEnv

	void setupSpec(){
		testEnv = new Environment()
		testEnv.addDispatcher('eventLoop',new BlockingQueueDispatcher('eventLoop', 256))
	}

	def "Compose from Closure"() {
		when: 'Defer a composition'
		def c = Promises.task { sleep 500; 1 } get()

		and: 'apply a transformation'
		def d = c | { it + 1 }

		then: 'Composition contains value'
		d.await 5, TimeUnit.SECONDS
		d.get() == 2
	}

	def "Compose from multiple values"() {
		when: 'Defer a composition'
		def c = S.each(['1', '2', '3', '4', '5']).get()

		and: 'apply a transformation'
		int sum = 0
		def d = c | { Integer.parseInt it } | { sum += it; sum }

		then:
		d.await 1, TimeUnit.SECONDS
		sum == 15
	}

	def "Compose from multiple filtered values"() {
		when: 'Defer a composition'
		def c = S.each(['1', '2', '3', '4', '5']).get()

		and: 'apply a transformation that filters odd elements'
		def d = (c | { Integer.parseInt it }) & { it % 2 == 0 }

		then:
		d.await 1, TimeUnit.SECONDS
		d.get() == 4
	}

	def "Error handling with composition from multiple values"() {
		when: 'Defer a composition'
		def c = S.each(['1', '2', '3', '4', '5']).get()

		and: 'apply a transformation that generates an exception for the last value'
		int sum = 0
		def d = c | { Integer.parseInt it } | { if (it >= 5) throw new IllegalArgumentException() else sum += it }

		then:
		d.await 100, TimeUnit.SECONDS
		d.get() == 10
	}


	def "Value is immediately available"() {
		when: 'Defer a composition'
		def c = S.each(['1', '2', '3', '4', '5']).get()

		then:
		c.get() == '5'
	}


	def "Reduce composition from multiple values"() {
		when: 'Defer a composition'
		def c = S.each(['1', '2', '3', '4', '5']).get()

		and: 'apply a reduction'
		def d = (c | { Integer.parseInt it }) % { i, acc = 1 -> acc * i }

		then:
		d.await 1, TimeUnit.SECONDS
		d.get() == 120
	}


	def "consume first and last with a composition from multiple values"() {
		when: 'Defer a composition'
		def c = S.each(['1', '2', '3', '4', '5']).get()

		and: 'apply a transformation'
		def d = c | { Integer.parseInt it }

		and: 'reference first and last'
		def first = d.first()
		def last = d.last()

		then:
		first.await(1, TimeUnit.SECONDS) == 1
		last.await(1, TimeUnit.SECONDS) == 5
	}


	def "Compose events (Request/Reply)"() {
		given: 'a reactor and a selector'
		def r = R.reactor().using(testEnv).dispatcher('eventLoop').get()
		def key = $()

		when: 'register a Reply Consumer'
		r.receive(key.t1) { String test ->
			Integer.parseInt test
		}

		and: 'compose the event'
		def c = r.compose(key.t2, '1') % { i, acc = [] -> acc << i }

		then:
		c.await(1, TimeUnit.SECONDS)
		c.get() == [1]
	}


	def "Compose events (Request/ N Replies)"() {
		given: 'a reactor and a selector'
		def r = R.reactor().using(testEnv).dispatcher('eventLoop').get()
		def key = $()

		when: 'register a Reply Consumer'
		r.receive(key.t1) { String test ->
			Integer.parseInt test
		}
		r.receive(key.t1) { String test ->
			(Integer.parseInt(test)) * 100
		}

		r.receive(key.t1) { String test ->
			(Integer.parseInt(test)) * 1000
		}

		and: 'prepare reduce and notify composition'
		def c1 = S.defer().using(r).get()
		def c2 = c1.take(2).reduce { i, acc = [] -> acc << i }

		r.compose(key.t2, '1', c1)

		then:
		c2.await(1, TimeUnit.SECONDS)
		c2.get() == [1, 100]

		when: 'using reduce() alias'
		c1 = S.defer().using(r).get()
		c2 = c1.take(3).reduce()

		r.compose(key.t2, '1', c1)

		then:
		c2.await(1, TimeUnit.SECONDS)
		c2.get() == [1, 100, 1000]
	}

	def "relay events to reactor"() {
		given: 'a reactor and a selector'
		def r = R.reactor().using(testEnv).dispatcher('eventLoop').get()
		def key = $()

		when: 'we consume when this reactor and key'
		def latch = new CountDownLatch(5)
		r.on(key.t1) {
			latch.countDown()
		}

		and: 'Defer a composition'
		def c = S.each(['1', '2', '3', '4', '5']).get()

		and: 'apply a transformation and call an explicit reactor'
		def d = (c | { Integer.parseInt it }).to(key.t2, r)
		d.get()

		then:
		latch.await(1, TimeUnit.SECONDS)
		latch.count == 0
		d.get() == 5
	}

	def "compose from unknown number of values"() {

		when: 'Defer a composition'
		def c = S.each(new TestIterable('1', '2', '3', '4', '5')).get()

		and: 'apply a transformation and call an explicit reactor'
		def sum = 0
		def d = c | { Integer.parseInt it } | { sum += it; sum }

		and:
		Thread.start {
			sleep 500
			c.expectedAcceptCount = 5
		}

		then:
		d.await(1, TimeUnit.SECONDS)
		d.get() == 15
	}

	static class TestIterable<T> implements Iterable<T> {

		private final Collection<T> items;

		@SafeVarargs
		public TestIterable(T... items) {
			this.items = Arrays.asList(items);
		}

		@Override
		public Iterator<T> iterator() {
			return this.items.iterator();
		}

	}

}
