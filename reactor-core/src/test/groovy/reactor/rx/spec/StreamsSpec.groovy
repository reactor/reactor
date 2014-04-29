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
package reactor.rx.spec

import reactor.core.Environment
import reactor.core.Observable
import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.event.selector.Selectors
import reactor.function.Function
import reactor.function.Supplier
import reactor.rx.Stream
import reactor.tuple.Tuple2
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import static reactor.GroovyTestUtils.*

class StreamsSpec extends Specification {

	def 'A deferred Stream with an initial value makes that value available immediately'() {
		given:
			'a composable with an initial value'
			Stream stream = Streams.defer('test')

		when:
			'the value is retrieved'
			def value = stream.tap()

		then:
			'it is available'
			value.get() == 'test'
	}

	def 'A deferred Stream with a generated value makes that value available immediately'() {
		given:
			String test = ""
			'a composable with an initial value'
			Stream stream = Streams.defer(supplier { test })

		when:
			'the value is retrieved'
			test = "test"
			def value = stream.tap()

		then:
			'it is available'
			value.get() == 'test'

		when:
			'nothing is provided'
			Streams.defer((Supplier<Object>) null)

		then:
			"exception is thrown"
			thrown(IllegalArgumentException)
	}

	def 'A Stream with a known set of values makes those values available immediately'() {
		given:
			'a composable with values 1 to 5 inclusive'
			Stream s = Streams.defer([1, 2, 3, 4, 5])

		when:
			'the first value is retrieved'
			def first = s.first().tap()

		and:
			'the last value is retrieved'
			def last = s.last().tap()

		then:
			'first and last'
			first.get() == 1
			last.get() == 5
	}

	def 'A Stream can be enforced to dispatch distinct values'() {
		given:
			'a composable with values 1 to 3 with duplicates'
			Stream s = Streams.defer([1, 1, 2, 2, 3])

		when:
			'the values are filtered and result is collected'
			def tap = s.distinctUntilChanged().collect().tap()

		then:
			'collected must remove duplicates'
			tap.get() == [1,2,3]
	}

	def 'A Stream with an unknown set of values makes those values available when flush predicate agrees'() {
		given:
			'a composable to defer'
			Stream s = Streams.<Integer> defer()

		when:
			'a flushable propagate action is attached'
			def tap = s.propagate(supplier {5}).filter(predicate { println it; it == 5 }).tap()

		and:
			'a start trigger and a filtered tap are attached'
			s.flushWhen(predicate { it == 1 })

		and:
			'values are accepted'
			println s.debug()
			s.broadcastNext(1)
			println s.debug()

		then:
			'the filtered tap should see 5 from the propagated values'
			tap.get() == 5

	}

	def "A Stream's initial values are passed to consumers"() {
		given:
			'a composable with values 1 to 5 inclusive'
			Stream stream = Streams.defer([1, 2, 3, 4, 5])

		when:
			'a Consumer is registered'
			def values = []
			stream.consume(consumer { values << it })

		then:
			'the initial values are passed'
			values == [1, 2, 3, 4, 5]


	}

	def 'Accepted values are passed to a registered Consumer'() {
		given:
			'a composable with a registered consumer'
			def composable = Streams.<Integer>defer()
			def value = composable.tap()

		when:
			'a value is accepted'
			composable.broadcastNext(1)

		then:
			'it is passed to the consumer'
			value.get() == 1

		when:
			'another value is accepted'
			composable.broadcastNext(2)

		then:
			'it too is passed to the consumer'
			value.get() == 2
	}

	def 'Accepted errors are passed to a registered Consumer'() {
		given:
			'a composable with a registered consumer of RuntimeExceptions'
			Stream composable = Streams.<Integer>defer()
			def errors = 0
			composable.when(RuntimeException, consumer { errors++ })
			println composable.debug()

		when:
			'A RuntimeException is accepted'
			composable.broadcastError(new RuntimeException())

		then:
			'it is passed to the consumer'
			errors == 1

		when:
			'A new error consumer is subscribed'
			composable.when(RuntimeException, consumer { errors++ })

		then:
			'it is called since publisher is in error state'
			errors == 2

		when:
			'A RuntimeException is accepted'
			composable.broadcastError(new IllegalArgumentException())

		then:
			'it is not passed to the consumer'
			errors == 2
	}

	def 'When the accepted event is Iterable, split can iterate over values'() {
		given:
			'a composable with a known number of values'
			def d = Streams.<Iterable<String>> defer()
			Stream<String> composable = d.split()

		when:
			'accept list of Strings'
			def tap = composable.tap()
			d.broadcastNext(['a', 'b', 'c'])

		then:
			'its value is the last of the initial values'
			tap.get() == 'c'

	}

	def 'Last value of a batch is accessible'() {
		given:
			'a composable that will accept an unknown number of values'
			def d = Streams.<Integer>config().batchSize(3).get()
			Stream composable = d

		when:
			'the expected accept count is set and that number of values is accepted'
			def tap = composable.last().tap()
			d.broadcastNext(1)
			d.broadcastNext(2)
			d.broadcastNext(3)

		then:
			"last's value is now that of the last value"
			tap.get() == 3

		when:
			'the expected accept count is set and that number of values is accepted'
			tap = composable.last(3).tap()
			d.broadcastNext(1)
			d.broadcastNext(2)
			d.broadcastNext(3)

		then:
			"last's value is now that of the last value"
			tap.get() == 3
	}

	def "A Stream's values can be mapped"() {
		given:
			'a source composable with a mapping function'
			def source = Streams.<Integer>defer()
			Stream mapped = source.map(function { it * 2 })

		when:
			'the source accepts a value'
			def value = mapped.tap()
			source.broadcastNext(1)

		then:
			'the value is mapped'
			value.get() == 2
	}

	def "Stream's values can be exploded"() {
		given:
			'a source composable with a mapMany function'
			def source = Streams.<Integer> defer()
			Stream<Integer> mapped = source.
					flatMap(function { Integer v -> Streams.<Integer> defer(v * 2) })

		when:
			'the source accepts a value'
			println source.debug()
			def value = mapped.tap()
			source.broadcastNext(1)
			println source.debug()

		then:
			'the value is mapped'
			value.get() == 2
	}

	def "Multiple Stream's values can be merged"() {
		given:
			'source composables to merge, collect and tap'
			def source1 = Streams.<Integer> defer()
			def source2 = Streams.<Integer> defer()
			def source3 = Streams.<Integer> defer()
			def tap = source1.merge(source2, source3).collect(3).tap()

		when:
			'the sources accept a value'
			source1.broadcastNext(1)
			source2.broadcastNext(2)
			source3.broadcastNext(3)

		then:
			'the values are all collected from source1 stream'
			tap.get() == [1,2,3]
	}


	def "Stream can be counted"() {
		given:
			'source composables to count and tap'
			def source = Streams.<Integer> defer()
			def tap = source.count().valueStream().tap()

		when:
			'the sources accept a value'
			source.broadcastNext(1)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastFlush()

		then:
			'the count value matches the number of accept'
			tap.get() == 3
	}

	def "A Stream's values can be filtered"() {
		given:
			'a source composable with a filter that rejects odd values'
			def source = Streams.<Integer>defer()
			Stream filtered = source.filter(predicate { it % 2 == 0 })

		when:
			'the source accepts an even value'
			def value = filtered.tap()
			source.broadcastNext(2)

		then:
			'it passes through'
			value.get() == 2

		when:
			'the source accepts an odd value'
			source.broadcastNext(3)

		then:
			'it is blocked by the filter'
			value.get() == 2

		when:
			'add a rejected stream'
			def rejectedSource = filtered.filter(predicate { false }).otherwise()
			def rejectedTap = rejectedSource.tap()
			source.broadcastNext(2)
			println source.debug()

		then:
			'it is rejected by the filter'
			rejectedTap.get() == 2

		when:
			'simple filter'
			anotherSource = Streams.<Boolean> defer()
			tap = anotherSource.filter().tap()
			anotherSource.broadcastNext(true)

		then:
			'it is accepted by the filter'
			tap.get()

		when:
			'simple filter nominal case'
			anotherSource = Streams.<Boolean> defer()
			tap = anotherSource.filter().tap()
			anotherSource.broadcastNext(false)

		then:
			'it is not accepted by the filter'
			!tap.get()
	}

	def "When a mapping function throws an exception, the mapped composable accepts the error"() {
		given:
			'a source composable with a mapping function that throws an exception'
			def source = Streams.<Integer>defer()
			Stream mapped = source.map(function { if(it==1) throw new RuntimeException() else 'na' })
			def errors = 0
			mapped.when(Exception, consumer { errors++ })
			mapped.resume()

		when:
			'the source accepts a value'
			source.broadcastNext(1)
		println source.debug()

		then:
			'the error is passed on'
			errors == 1
	}

	def "When a filter function throws an exception, the filtered composable accepts the error"() {
		given:
			'a source composable with a filter function that throws an exception'
			def source = Streams.<Integer>defer()
			Stream filtered = source.filter(predicate { if(it == 1) throw new RuntimeException() else true })
			def errors = 0
			filtered.when(Exception, consumer { errors++ })
			filtered.resume()

		when:
			'the source accepts a value'
			source.broadcastNext(1)

		then:
			'the error is passed on'
			errors == 1
	}

	def "A known set of values can be reduced"() {
		given:
			'a composable with a known set of values'
			Stream source = Streams.defer([1, 2, 3, 4, 5])

		when:
			'a reduce function is registered'
			def reduced = source.reduce(new Reduction())
			def value = reduced.tap()

		then:
			'the resulting composable holds the reduced value'
			value.get() == 120

		when:
			'use an initial value'
			value = source.reduce(new Reduction(), 2).tap()
		println source.debug()

		then:
			'the updated reduction is available'
			value.get() == 240
	}

	def "When reducing a known set of values, only the final value is passed to consumers"() {
		given:
			'a composable with a known set of values and a reduce function'
			Stream reduced = Streams.<Integer>config().each([1, 2, 3, 4, 5]).
					synchronousDispatcher().
					get().
					reduce(new Reduction())

		when:
			'a consumer is registered'
			def values = []
			reduced.consume(consumer { values << it })

		then:
			'the consumer only receives the final value'
			values == [120]
	}

	def "When reducing a known number of values, only the final value is passed to consumers"() {
		given:
			'a composable with a known number of values and a reduce function'
			def source = Streams.<Integer>config().batchSize(5).get()
			Stream reduced = source.reduce(new Reduction())
			def values = []
			reduced.consume(consumer { values << it })

		when:
			'the expected number of values is accepted'
			source.broadcastNext(1)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastNext(4)
			source.broadcastNext(5)

		then:
			'the consumer only receives the final value'
			values == [120]
	}

	def 'A known number of values can be reduced'() {
		given:
			'a composable that will accept 5 values and a reduce function'
			def source = Streams.<Integer> config().batchSize(5).get()
			Stream reduced = source.reduce(new Reduction())
			def value = reduced.tap()

		when:
			'the expected number of values is accepted'
			source.broadcastNext(1)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastNext(4)
			source.broadcastNext(5)

		then:
			'the reduced composable holds the reduced value'
			value.get() == 120
	}

	def 'When a known number of values is being reduced, only the final value is made available'() {
		given:
			'a composable that will accept 2 values and a reduce function'
			def source = Streams.<Integer> config().batchSize(2).get()
			def value = source.reduce(new Reduction()).tap()

		when:
			'the first value is accepted'
			source.broadcastNext(1)

		then:
			'the reduced value is unknown'
			value.get() == null

		when:
			'the second value is accepted'
			source.broadcastNext(2)

		then:
			'the reduced value is known'
			value.get() == 2
	}

	def 'When an unknown number of values is being reduced, each reduction is passed to a consumer on flush'() {
		given:
			'a composable with a reduce function'
			def source = Streams.<Integer> defer()
			Stream reduced = source.reduce(new Reduction())
			def value = reduced.tap()

		when:
			'the first value is accepted'
			source.broadcastNext(1)

		then:
			'the reduction is not available'
			!value.get()

		when:
			'the second value is accepted and flushed'
			source.broadcastNext(2)
			reduced.start()

		then:
			'the updated reduction is available'
			value.get() == 2

		when:
			'use an initial value'
			reduced = source.reduce(new Reduction(), 2)
			value = reduced.tap()
			source.broadcastNext(1)
			reduced.start()

		then:
			'the updated reduction is available'
			value.get() == 2
	}

	def 'When an unknown number of values is being scanned, each reduction is passed to a consumer'() {
		given:
			'a composable with a reduce function'
			def source = Streams.<Integer> defer()
			Stream reduced = source.scan(new Reduction())
			def value = reduced.tap()

		when:
			'the first value is accepted'
			source.broadcastNext(1)

		then:
			'the reduction is available'
			value.get() == 1

		when:
			'the second value is accepted'
			source.broadcastNext(2)

		then:
			'the updated reduction is available'
			value.get() == 2

		when:
			'use an initial value'
			value = source.scan(new Reduction(), 4).tap()
			source.broadcastNext(1)

		then:
			'the updated reduction is available'
			value.get() == 4
	}



	def 'Reduce will accumulate a list of accepted values'() {
		given:
			'a composable'
			def source = Streams.<Integer>config().batchSize(1).get()
			Stream reduced = source.collect()
			def value = reduced.tap()

		when:
			'the first value is accepted'
		println reduced.debug()
			source.broadcastNext(1)
			println reduced.debug()

		then:
			'the list contains the first element'
			value.get() == [1]
	}

	def 'Collect will accumulate a list of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer>config().batchSize(2).get()
			Stream reduced = source.collect()
			def value = reduced.tap()

		when:
			'the first value is accepted on the source'
			source.broadcastNext(1)

		then:
			'the collected list is not yet available'
			value.get() == null

		when:
			'the second value is accepted'
			source.broadcastNext(2)

		then:
			'the collected list contains the first and second elements'
			value.get() == [1, 2]
	}

	def 'Collect will accumulate a list of accepted values until flush and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer>defer()
			Stream reduced = source.collect()
			def value = reduced.tap()

		when:
			'the first value is accepted on the source'
			source.broadcastNext(1)

		then:
			'the collected list is not yet available'
			value.get() == null

		when:
			'the second value is accepted'
			source.broadcastNext(2)
			source.broadcastFlush()

		then:
			'the collected list contains the first and second elements'
			value.get() == [1, 2]
	}

	def 'Creating Stream from environment'() {
		given:
			'a source stream with a given environment'
			Environment environment = new Environment()
			def source = Streams.<Integer> defer(environment, environment.getDispatcher('ringBuffer'))
			def source2 = Streams.<Integer> defer(environment)

		when:
			'accept a value'
			CountDownLatch latch = new CountDownLatch(2)
			def v = ""
			source.consume(consumer { v = 'ok'; latch.countDown() })
			source2.consume(consumer { v = 'ok'; latch.countDown() })
			source.broadcastNext(1)
			source2.broadcastNext(1)

		then:
			'dispatching works'
			latch.await(1000, TimeUnit.MILLISECONDS)
			v == 'ok'


		cleanup:
			environment.shutdown()
	}

	def 'Creating Stream from observable'() {
		given:
			'a source stream with a given observable'
			def r = Reactors.reactor().get()
			def selector = Selectors.anonymous()
			int event = 0
			Streams.<Integer> on(r, selector).consume(consumer { event = it })

		when:
			'accept a value'
			r.notify(selector.object, Event.wrap(1))

		then:
			'dispatching works'
			event == 1
	}

	def 'Window will accumulate a list of accepted values and pass it to a consumer on the specified period'() {
		given:
			'a source and a collected stream'
			Environment environment = new Environment()
			def source = Streams.<Integer> config().synchronousDispatcher().env(environment).get()
			Stream reduced = source.window(500)
			def value = reduced.tap()

		when:
			'the first values are accepted on the source'
			source.broadcastNext(1)
			source.broadcastNext(2)
			sleep(1200)

		then:
			'the collected list is available'
			value.get() == [1, 2]

		when:
			'the second value is accepted'
			source.broadcastNext(3)
			source.broadcastNext(4)
			sleep(1200)

		then:
			'the collected list contains the first and second elements'
			value.get() == [3, 4]

		cleanup:
			environment.shutdown()

	}

	def 'Collect with Timeout will accumulate a list of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			Environment environment = new Environment()
			def source = Streams.<Integer> config().synchronousDispatcher().env(environment).get()
			Stream reduced = source.collect(5).timeout(1000)
			def value = reduced.tap()
println reduced.debug()
		when:
			'the first values are accepted on the source'
			source.broadcastNext(1)
			source.broadcastNext(1)
			source.broadcastNext(1)
			source.broadcastNext(1)
			source.broadcastNext(1)
			println reduced.debug()

		then:
			'the collected list is not yet available'
			value.get() == [1, 1, 1, 1, 1]

		when:
			'the second value is accepted'
			source.broadcastNext(2)
			source.broadcastNext(2)
			sleep(1500)

		then:
			'the collected list contains the first and second elements'
			value.get() == [2, 2]

		cleanup:
			environment.shutdown()

	}

	def 'Moving Window accumulate items without dropping previous'() {
		given:
			'a source and a collected stream'
			Environment environment = new Environment()
			def source = Streams.<Integer> config().synchronousDispatcher().env(environment).get()
			Stream reduced = source.movingWindow(500, 5)
			def value = reduced.tap()

		when:
			'the window accepts first items'
			source.broadcastNext(1)
			source.broadcastNext(2)
			sleep(1200)

		then:
			'it outputs received values'
			value.get() == [1, 2]

		when:
			'the window accepts following items'
			source.broadcastNext(3)
			source.broadcastNext(4)
			sleep(1200)

		then:
			'it outputs received values'
			value.get() == [1, 2, 3, 4]

		when:
			'the starts dropping items on overflow'
			source.broadcastNext(5)
			source.broadcastNext(6)
			sleep(1200)

		then:
			'it outputs received values'
			value.get() == [2, 3, 4, 5, 6]

		cleanup:
			environment.shutdown()
	}

	def 'Moving Window will drop overflown items'() {
		given:
			'a source and a collected stream'
			Environment environment = new Environment()
			def source = Streams.<Integer> config().synchronousDispatcher().env(environment).get()
			Stream reduced = source.movingWindow(500, 5)
			def value = reduced.tap()

		when:
			'the window overflows'
			source.broadcastNext(1)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastNext(4)
			source.broadcastNext(5)
			source.broadcastNext(6)
			sleep(1200)

		then:
			'it outputs values dismissing outdated ones'
			value.get() == [2, 3, 4, 5, 6]

		cleanup:
			environment.shutdown()
	}

	def 'Collect will accumulate values from multiple threads'() {
		given:
			'a source and a collected stream'
			def sum = new AtomicInteger()
			def latch = new CountDownLatch(3)
			Environment env = new Environment()
			Stream head = Streams.<Integer>config().
					env(env).
					batchSize(333).
					dispatcher(Environment.THREAD_POOL).
					get()
			Stream tail = head.collect()
			tail.consume(consumer { List<Integer> ints ->
				println ints.size()
				sum.addAndGet(ints.size())
				latch.countDown()
			})

		when:
			'values are accepted into the head'
			(1..1000).each { head.broadcastNext(it) }

		then:
			'results contains the expected values'
			try{
			latch.await(5, TimeUnit.SECONDS)
			}catch(e){}
			sum.get() == 999
	}

	def 'An Observable can consume values from a Stream'() {
		given:
			'a Stream and a Observable consumer'
			def d = Streams.<Integer>defer()
			Stream composable = d
			Observable observable = Mock(Observable)
			composable.consume('key', observable)

		when:
			'the composable accepts a value'
			d.broadcastNext(1)

		then:
			'the observable is notified'
			1 * observable.notify('key', _)
	}

	def 'An observable can consume values from a Stream with a known set of values'() {
		given:
			'a Stream with 3 values'
			Stream stream = Streams.defer([1, 2, 3])
			Observable observable = Mock(Observable)

		when:
			'a stream consumer is registerd'
			stream.consume('key', observable)

		then:
			'the observable is notified of the values'
			3 * observable.notify('key', _)
	}

	static class Reduction implements Function<Tuple2<Integer, Integer>, Integer> {
		@Override
		public Integer apply(Tuple2<Integer, Integer> reduce) {
			def result = reduce.t2 == null ? 1 : reduce.t1 * reduce.t2
			println "${reduce?.t2} ${reduce?.t1} reduced to ${result}"
			return result
		}
	}
}
