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
import reactor.event.EventBus
import reactor.function.support.Tap
import reactor.rx.Stream
import reactor.rx.Streams
import reactor.tuple.Tuple2
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.event.selector.Selectors.$

/**
 * @author Stephane Maldini
 */
class GroovyStreamSpec extends Specification {

	@Shared
	def testEnv

	void setupSpec() {
		testEnv = new Environment()
	}


	def "Compose from multiple values"() {
		when:
			'Defer a composition'
			def s = Streams.from(['1', '2', '3', '4', '5'])

		and:
			'apply a transformation'
			int sum = 0
			def d = s | { Integer.parseInt it } | { sum += it; sum }

		then:
			d.tap().get() == sum
			sum == 15
	}

	def "Compose from multiple filtered values"() {
		when:
			'Defer a composition'
			def c = Streams.from(['1', '2', '3', '4', '5'])

		and:
			'apply a transformation that filters odd elements'
			def t = new Tap<Integer>()
			((c | { Integer.parseInt it }) & { it % 2 == 0 }) << t

		then:
			t.get() == 4
	}

	def "Error handling with composition from multiple values"() {
		when:
			'Defer a composition'
			def c = Streams.from(['1', '2', '3', '4', '5'])

		and:
			'apply a transformation that generates an exception for the last value'
			int sum = 0
			def t = new Tap()
			def d = c | { Integer.parseInt it } | { if (it >= 5) throw new IllegalArgumentException() else sum += it }
			d << t

		then:
			t.get() == 10
	}


	def "Reduce composition from multiple values"() {
		when:
			'Defer a composition'
			def c = Streams.from([1, 2, 3, 4, 5])

		and:
			'apply a reduction'
			def d = c % { Tuple2<Integer,Integer> tuple2 -> tuple2.t1 * (tuple2.t2 ?: 1)}
			def t = d.tap()

		then:
			t.get() == 120
	}


	def "consume first and last with a composition from multiple values"() {
		when:
			'Defer a composition'
			def d = Streams.from([1, 2, 3, 4, 5])

		and:
			'apply a transformation'

		and:
			'reference first and last'
			def first = d.sampleFirst()
			def last = d.sample()

		then:
			first.tap().get() == 1
			last.tap().get() == 5
	}

	/*def "Compose events (Request/Reply)"() {
		given:
			'a eventBus and a selector'
			def r = EventBus.config().using(testEnv).dispatcher('eventLoop').get()
			def key = $()

		when:
			'register a Reply Consumer'
			r.receive(key.t1) { String test ->
				Integer.parseInt test
			}

		and:
			'compose the event'
			def c = r.compose(key.t2, '1') % { i, acc = [] -> acc << i }

		then:
			c.awaitNext(1, TimeUnit.SECONDS)
			c.get() == [1]
	}*/

	/* def "Compose events (Request/ N Replies)"() {
			given:
				'a eventBus and a selector'
				def r = EventBus.config().using(testEnv).dispatcher('eventLoop').get()
				def key = $()

			when:
				'register a Reply Consumer'
				r.receive(key.t1) { String test ->
					Integer.parseInt test
				}
				r.receive(key.t1) { String test ->
					(Integer.parseInt(test)) * 100
				}

				r.receive(key.t1) { String test ->
					(Integer.parseInt(test)) * 1000
				}

			and:
				'prepare reduce and notify composition'
				def c1 = Streams.generate().using(r).get()
				def c2 = c1.take(2).reduce { i, acc = [] -> acc << i }

				r.compose(key.t2, '1', c1)

			then:
				c2.awaitNext(1, TimeUnit.SECONDS)
				c2.get() == [1, 100]

			when:
				'using reduce() alias'
				c1 = Streams.generate().using(r).get()
				c2 = c1.take(3).reduce()

				r.compose(key.t2, '1', c1)

			then:
				c2.awaitNext(1, TimeUnit.SECONDS)
				c2.get() == [1, 100, 1000]
		}*/

	def "relay events to reactor"() {
		given:
			'a eventBus and a selector'
			def r = EventBus.config().env(testEnv).get()
			def key = $()

		when:
			'we connect when this eventBus and key'
			def latch = new CountDownLatch(5)
			r.on(key) {
				latch.countDown()
			}

		and:
			'Defer a composition'
			def c = Streams.from(['1', '2', '3', '4', '5'])

		and:
			'apply a transformation and call an explicit eventBus'
			(c | { Integer.parseInt it }).to(key.object, r)

		then:
			latch.await(1, TimeUnit.SECONDS)
			latch.count == 0
	}

	def "compose from unknown number of values"() {

		when:
			'Defer a composition'
			def c = Streams.from(new TestIterable('1', '2', '3', '4', '5'))

		and:
			'apply a transformation and call an explicit eventBus'
			def sum = 0
			Stream d = c | { Integer.parseInt it } | { sum += it; sum }

		and:
			'set a batch size to tap value after 5 iterations'
			def t = d.sample(5).tap()

		then:
			t.get()
			sum == 15
	}

	static class TestIterable<T> implements Iterable<T> {

		private final Collection<T> items;

		public TestIterable(T... items) {
			this.items = Arrays.asList(items);
		}

		@Override
		public Iterator<T> iterator() {
			return this.items.iterator();
		}

	}

}
