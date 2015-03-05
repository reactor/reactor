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
package reactor.groovy

import reactor.Environment
import reactor.fn.support.Tap
import reactor.fn.tuple.Tuple
import reactor.rx.Stream
import reactor.rx.Streams
import spock.lang.Shared
import spock.lang.Specification

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


	def "Reduce By Key on Tuple Stream from multiple values"() {
		when:
			'Defer a composition'
			def s = Streams.just(
					Tuple.of('1', 1),
					Tuple.of('1', 1),
					Tuple.of('2', 1),
					Tuple.of('1', 2),
					Tuple.of('3', 1),
					Tuple.of('3', 1),
			)

		and:
			'apply a transformation'
			def d = s.
					reduceByKey{ prev, acc -> prev + acc}.
					sort{ a, b -> b.t2 <=> a.t2 }.
					map { it.t1 }.
					toList().
					await()

		then:
			d == ['1', '3', '2']
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
			def d = c % { acc, next -> next * acc}
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
