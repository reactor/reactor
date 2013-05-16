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

import reactor.core.Composable
import reactor.core.R
import reactor.core.Reactor
import reactor.fn.Consumer
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.Fn.$

/**
 * @author Stephane Maldini
 */
class GroovyComposableSpec extends Specification {

	def "Compose from single value"() {
		when: 'Defer a composition'
		def c = Composable.from("Hello World!").build()

		and: 'apply a transformation'
		def d = c.map {
				 'Goodbye then!'
		}

		then: 'Composition contains value'
		d.await 1, TimeUnit.SECONDS
		d.get() == 'Goodbye then!'
	}

	def "Compose from multiple values"() {
		when: 'Defer a composition'
		def c = Composable.from(['1','2','3','4','5']).build()

		and: 'apply a transformation'
		int sum = 0
		def d = c | { Integer.parseInt it } | { sum += it; sum }

		then:
		d.await 1, TimeUnit.SECONDS
		sum == 15
	}

	def "Compose from multiple filtered values"() {
		when: 'Defer a composition'
		def c = Composable.from(['1','2','3','4','5']).build()

		and: 'apply a transformation that filters odd elements'
		def d = (c | { Integer.parseInt it }) & { it % 2 == 0 }

		then:
		d.await 1, TimeUnit.SECONDS
		d.get() == 4
	}

	def "Error handling with composition from multiple values"() {
		when: 'Defer a composition'
		def c = Composable.from(['1','2','3','4','5']).build()

		and: 'apply a transformation that generates an exception for the last value'
		int sum = 0
		def d = c | { Integer.parseInt it } | { if(it >= 5) throw new IllegalArgumentException() else sum += it }

		then:
		d.await 1, TimeUnit.SECONDS
		d.get() == 10
	}


	def "Value is immediately available"() {
		when: 'Defer a composition'
		def c = Composable.from(['1','2','3','4','5']).build()

		then:
		c.get() == '5'
	}



	def "Reduce composition from multiple values"() {
		when: 'Defer a composition'
		def c = Composable.from(['1','2','3','4','5']).build()

		and: 'apply a reduction'
		def d =( c | { Integer.parseInt it } ) % { i, acc = 1 -> acc * i}

		then:
		d.await 1, TimeUnit.SECONDS
		d.get() == 120
	}


	def "consume first and last with a composition from multiple values"() {
		when: 'Defer a composition'
		def c = Composable.from(['1','2','3','4','5']).build()

		and: 'apply a transformation'
		def d = c | { Integer.parseInt it }

		and: 'reference first and last'
		def first = d.first()
		def last = d.last()

		then:
		first.await(1, TimeUnit.SECONDS) == 1
		last.await(1, TimeUnit.SECONDS )== 5
	}



	def "Compose events (Request/Reply)"() {
		given: 'a reactor and a selector'
		def r = R.create()
		def key = $(new Object())

		when: 'register a Reply Consumer'
		r.receive(key){String test->
			Integer.parseInt test
		}

		and: 'compose the event'
		def c = r.compose(key.object, '1') % {i, acc = [] -> acc << i}

		then:
		c.await(1, TimeUnit.SECONDS)
		c.get() == [1]
	}


	def "Compose events (Request/ N Replies)"() {
		given: 'a reactor and a selector'
		def r = R.create()
		def key = $(new Object())

		when: 'register a Reply Consumer'
		r.receive(key){String test->
			Integer.parseInt test
		}
		r.receive(key){String test->
			(Integer.parseInt(test))*100
		}

		and: 'prepare reduce and notify composition'
		def c1 = Composable.lazy().using(r).build()
		def c2 = c1.take(2).reduce{i, acc = [] -> acc <<	i }

		r.compose(key.object, '1', c1)

		then:
		c2.await(5, TimeUnit.SECONDS)
		c2.get() == [1,100]
	}

	def "relay events to reactor"() {
		given: 'a reactor and a selector'
		def r = R.create()
		def key = $(new Object())

		when: 'we consume from this reactor and key'
		def latch = new CountDownLatch(5)
		r.on(key){
			latch.countDown()
		}

		and: 'Defer a composition'
		def c = Composable.from(['1','2','3','4','5']).build()

		and: 'apply a transformation and call an explicit reactor'
		def d = (c | { Integer.parseInt it }).to(key.object, r)
		d.get()

		then:
		latch.await(1, TimeUnit.SECONDS)
		latch.count == 0
		d.get() == 5
	}

	def "compose from unknown number of values"() {

		when: 'Defer a composition'
		def c = Composable.from(new TestIterable('1','2','3','4','5')).build()

		and: 'apply a transformation and call an explicit reactor'
		def sum = 0
		def d = c | { Integer.parseInt it } | {sum += it; sum}

		and:
		Thread.start{
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
