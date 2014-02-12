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
package reactor.core.composable.spec

import reactor.core.Environment
import reactor.core.composable.Composable
import reactor.core.composable.Deferred
import reactor.core.composable.Stream
import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.event.selector.Selectors
import reactor.function.Function
import reactor.core.Observable
import reactor.function.Supplier
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
			Stream stream = Streams.defer('test').synchronousDispatcher().get()

		when:
			'the value is retrieved'
			def value = stream.tap()
			stream.flush()

		then:
			'it is available'
			value.get() == 'test'
	}

	def 'A deferred Stream with an generated value makes that value available immediately'() {
		given:
			String test = ""
			'a composable with an initial value'
			Stream stream = Streams.defer(supplier { test }).synchronousDispatcher().get()

		when:
			'the value is retrieved'
			def value = stream.tap()
			test = "test"
			stream.flush()

		then:
			'it is available'
			value.get() == 'test'

		when:
			'nothing is provided'
			Streams.defer((Supplier<Object>) null).synchronousDispatcher().get()

		then:
			"exception is thrown"
			thrown(IllegalStateException)
	}

	def 'A Stream with a known set of values makes those values available immediately'() {
		given:
			'a composable with values 1 to 5 inclusive'
			Stream s = Streams.defer([1, 2, 3, 4, 5]).synchronousDispatcher().get()

		when:
			'the first value is retrieved'
			def first = s.first().tap()

		and:
			'the last value is retrieved'
			def last = s.last().tap()
			s.flush()

		then:
			'first and last'
			first.get() == 1
			println s.debug()
			last.get() == 5
	}

	def 'A Stream with an unknown set of values makes those values available when flush predicate agrees'() {
		given:
			'a composable to defer'
			Deferred d = Streams.<Integer> defer().synchronousDispatcher().get()
			Stream s = d.compose()

		when:
			'a flushable propagate action is attached'
			s.propagate([2, 3, 4, 5])

		and:
			'a flush trigger and a filtered tap are attached'
			def tap = s.flushWhen(predicate { it == 1 }).filter(predicate { it == 5 }).tap()

		and:
			'values are accepted'
			d.accept(1)

		then:
			'the filtered tap should see 5 from the propagated values'
			tap.get() == 5
			println s.debug()
	}

	def 'A Stream can be arbitrarely batched'() {
		given:
			'a composable to defer'
			Deferred d = Streams.<Integer> defer().synchronousDispatcher().get()
			Stream s = d.compose()

		when:
			'a batcher is created'
			def b = d.batcher()
			def tap = s.tap()

		and:
			'events are passed and a flush triggered'
			b.accept(Event.wrap(1))
			b.accept(Event.wrap(2))
			b.accept(Event.wrap(3))
			b.flush()

		then:
			'the filtered tap should see 5 from the propagated values'
			tap.get() == 3
			println s.debug()
	}


	def "A Stream's initial values are not passed to consumers but subsequent values are"() {
		given:
			'a composable with values 1 to 5 inclusive'
			Stream stream = Streams.defer([1, 2, 3, 4, 5]).synchronousDispatcher().get()

		when:
			'a Consumer is registered'
			def values = []
			stream.consume(consumer { values << it })

		then:
			'it is not called with the initial values'
			values == []

		when:
			'flush is called'
			stream.flush()

		then:
			'the initial values are passed'
			values == [1, 2, 3, 4, 5]


	}

	def 'Accepted values are passed to a registered Consumer'() {
		given:
			'a composable with a registered consumer'
			Deferred d = Streams.defer().synchronousDispatcher().get()
			Stream composable = d.compose()
			def value = composable.tap()

		when:
			'a value is accepted'
			d.accept(1)

		then:
			'it is passed to the consumer'
			value.get() == 1

		when:
			'another value is accepted'
			d.accept(2)

		then:
			'it too is passed to the consumer'
			value.get() == 2
	}

	def 'Accepted errors are passed to a registered Consumer'() {
		given:
			'a composable with a registered consumer of RuntimeExceptions'
			Deferred d = Streams.defer().synchronousDispatcher().get()
			Composable composable = d.compose()
			def errors = 0
			composable.when(RuntimeException, consumer { errors++ })
			println composable.debug()

		when:
			'A RuntimeException is accepted'
			d.accept(new RuntimeException())

		then:
			'it is passed to the consumer'
			errors == 1

		when:
			'A checked exception is accepted'
			d.accept(new Exception())

		then:
			'it is not passed to the consumer'
			errors == 1

		when:
			'A subclass of RuntimeException is accepted'
			d.accept(new IllegalArgumentException())

		then:
			'it is passed to the consumer'
			errors == 2
	}

	def 'A Stream can connect values and errors from another Stream'() {
		given:
			'a deferred composable with a consuming Stream'
			Deferred<Integer, Stream<Integer>> parent = Streams.<Integer> defer().synchronousDispatcher().get()
			Deferred<Integer, Stream<Integer>> child = Streams.<Integer> defer().synchronousDispatcher().get()
			Stream s = child.compose()

			def error
			def value = s.tap()
			s.when(Exception, consumer { error = it })
			parent.compose().connect(s)

		when:
			'the parent accepts a value'
			parent.accept(1)

		then:
			'it is passed to the child'
			1 == value.get()

		when:
			"the parent accepts an error and the child's value is accessed"
			parent.accept(new Exception())

		then:
			'the child contains the error from the parent'
			error instanceof Exception
	}

	def 'A Stream can consume values from another Stream'() {
		given:
			'a deferred composable with a consuming Stream'
			Deferred<Integer, Stream<Integer>> parent = Streams.<Integer> defer().synchronousDispatcher().get()
			Deferred<Integer, Stream<Integer>> child = Streams.<Integer> defer().synchronousDispatcher().get()
			Stream s = child.compose()

			def error
			def value = s.tap()
			s.when(Exception, consumer { error = it })
			parent.compose().consume(s)

		when:
			'the parent accepts a value'
			parent.accept(1)

		then:
			'it is passed to the child'
			1 == value.get()

		when:
			"the parent accepts an error and the child's value is accessed"
			parent.accept(new Exception())

		then:
			'the child does not contains the error from the parent'
			!error

		when:
			"try to consume itself"
			parent.compose().consume(parent.compose())

		then:
			'the child contains the error from the parent'
			thrown IllegalArgumentException
	}

	def 'When the accepted event is Iterable, split can iterate over values'() {
		given:
			'a composable with a known number of values'
			Deferred<Iterable<String>, Stream<Iterable<String>>> d = Streams.<Iterable<String>> defer().get()
			Stream<String> composable = d.compose().split()

		when:
			'accept list of Strings'
			def tap = composable.tap()
			d.accept(['a', 'b', 'c'])

		then:
			'its value is the last of the initial values'
			tap.get() == 'c'

	}

	def 'When the number of values is unknown, last is never updated'() {
		given:
			'a composable that will accept an unknown number of values'
			Deferred d = Streams.defer().get()
			Stream composable = d.compose()

		when:
			'last is retrieved'
			composable.last().tap()

		then:
			"can't call last() on unbounded stream"
			thrown(IllegalStateException)

	}

	def 'Last value of a batch is accessible'() {
		given:
			'a composable that will accept an unknown number of values'
			Deferred d = Streams.defer().batchSize(3).get()
			Stream composable = d.compose()

		when:
			'the expected accept count is set and that number of values is accepted'
			def tap = composable.last().tap()
			d.accept(1)
			d.accept(2)
			d.accept(3)

		then:
			"last's value is now that of the last value"
			tap.get() == 3

		when:
			'the expected accept count is set and that number of values is accepted'
			tap = composable.last(3).tap()
			d.accept(1)
			d.accept(2)
			d.accept(3)

		then:
			"last's value is now that of the last value"
			tap.get() == 3
	}

	def "A Stream's values can be mapped"() {
		given:
			'a source composable with a mapping function'
			Deferred source = Streams.defer().get()
			Stream mapped = source.compose().map(function { it * 2 })

		when:
			'the source accepts a value'
			def value = mapped.tap()
			source.accept(1)

		then:
			'the value is mapped'
			value.get() == 2
	}

	def "A Stream's values can be exploded"() {
		given:
			'a source composable with a mapMany function'
			Deferred source = Streams.<Integer> defer().get()
			Stream<Integer> mapped = source.compose().mapMany(function { Streams.<Integer> defer(it * 2).get() })

		when:
			'the source accepts a value'
			def value = mapped.tap()
			source.accept(1)

		then:
			'the value is mapped'
			value.get() == 2
	}

	def "A Stream's values can be filtered"() {
		given:
			'a source composable with a filter that rejects odd values'
			Deferred source = Streams.<Integer>defer().get()
			Stream filtered = source.compose().filter(predicate { it % 2 == 0 })

		when:
			'the source accepts an even value'
			def value = filtered.tap()
			source.accept(2)

		then:
			'it passes through'
			value.get() == 2

		when:
			'the source accepts an odd value'
			source.accept(3)

		then:
			'it is blocked by the filter'
			value.get() == 2

		when:
			'add a rejected stream'
			Deferred rejectedSource = Streams.<Integer> defer().get()
			def rejectedTap = rejectedSource.compose().tap()
			filtered.filter(predicate { false }, rejectedSource.compose())
			source.accept(2)
			println source.compose().debug()

		then:
			'it is rejected by the filter'
			rejectedTap.get() == 2

		when:
			'value-aware filter'
			Deferred anotherSource = Streams.<Integer> defer().get()
			def tap = anotherSource.compose().filter(function { it == 2 }).tap()
			anotherSource.accept(2)

		then:
			'it is accepted by the filter'
			tap.get() == 2

		when:
			'simple filter'
			anotherSource = Streams.<Boolean> defer().get()
			tap = anotherSource.compose().filter().tap()
			anotherSource.accept(true)

		then:
			'it is accepted by the filter'
			tap.get()

		when:
			'simple filter nominal case'
			anotherSource = Streams.<Boolean> defer().get()
			tap = anotherSource.compose().filter().tap()
			anotherSource.accept(false)

		then:
			'it is not accepted by the filter'
			!tap.get()
	}

	def "When a mapping function throws an exception, the mapped composable accepts the error"() {
		given:
			'a source composable with a mapping function that throws an exception'
			Deferred source = Streams.<Integer>defer().get()
			Stream mapped = source.compose().map(function { throw new RuntimeException() })
			def errors = 0
			mapped.when(Exception, consumer { errors++ })

		when:
			'the source accepts a value'
			source.accept(1)

		then:
			'the error is passed on'
			errors == 1
	}

	def "When a filter function throws an exception, the filtered composable accepts the error"() {
		given:
			'a source composable with a filter function that throws an exception'
			Deferred source = Streams.defer().synchronousDispatcher().get()
			Stream filtered = source.compose().filter(predicate { throw new RuntimeException() })
			def errors = 0
			filtered.when(Exception, consumer { errors++ })

		when:
			'the source accepts a value'
			source.accept(1)

		then:
			'the error is passed on'
			errors == 1
	}

	def "A known set of values can be reduced"() {
		given:
			'a composable with a known set of values'
			Stream source = Streams.defer([1, 2, 3, 4, 5]).synchronousDispatcher().get()

		when:
			'a reduce function is registered'
			def reduced = source.reduce(new Reduction())
			def value = reduced.tap()
			reduced.flush()

		then:
			'the resulting composable holds the reduced value'
			value.get() == 120

		when:
			'use an initial value'
			value = source.reduce(new Reduction(), 2).tap()
			source.flush()

		then:
			'the updated reduction is available'
			value.get() == 240
	}

	def "When reducing a known set of values, only the final value is passed to consumers"() {
		given:
			'a composable with a known set of values and a reduce function'
			Stream reduced = Streams.defer([1, 2, 3, 4, 5]).
					synchronousDispatcher().
					get().
					reduce(new Reduction())

		when:
			'a consumer is registered'
			def values = []
			reduced.consume(consumer { values << it }).flush()

		then:
			'the consumer only receives the final value'
			values == [120]
	}

	def "When reducing a known number of values, only the final value is passed to consumers"() {
		given:
			'a composable with a known number of values and a reduce function'
			Deferred source = Streams.defer().batchSize(5).synchronousDispatcher().get()
			Stream reduced = source.compose().reduce(new Reduction())
			def values = []
			reduced.consume(consumer { values << it })

		when:
			'the expected number of values is accepted'
			source.accept(1)
			source.accept(2)
			source.accept(3)
			source.accept(4)
			source.accept(5)

		then:
			'the consumer only receives the final value'
			values == [120]
	}

	def 'A known number of values can be reduced'() {
		given:
			'a composable that will accept 5 values and a reduce function'
			Deferred source = Streams.<Integer> defer().batchSize(5).synchronousDispatcher().get()
			Stream reduced = source.compose().reduce(new Reduction())
			def value = reduced.tap()

		when:
			'the expected number of values is accepted'
			source.accept(1)
			source.accept(2)
			source.accept(3)
			source.accept(4)
			source.accept(5)

		then:
			'the reduced composable holds the reduced value'
			value.get() == 120
	}

	def 'When a known number of values is being reduced, only the final value is made available'() {
		given:
			'a composable that will accept 2 values and a reduce function'
			Deferred source = Streams.<Integer> defer().batchSize(2).synchronousDispatcher().get()
			def value = source.compose().reduce(new Reduction()).tap()

		when:
			'the first value is accepted'
			source.accept(1)

		then:
			'the reduced value is unknown'
			value.get() == null

		when:
			'the second value is accepted'
			source.accept(2)

		then:
			'the reduced value is known'
			value.get() == 2
	}

	def 'When an unknown number of values is being reduced, each reduction is passed to a consumer on flush'() {
		given:
			'a composable with a reduce function'
			Deferred<Integer, Stream<Integer>> source = Streams.<Integer> defer().synchronousDispatcher().get()
			Stream reduced = source.compose().reduce(new Reduction())
			def value = reduced.tap()

		when:
			'the first value is accepted'
			source.accept(1)

		then:
			'the reduction is not available'
			!value.get()

		when:
			'the second value is accepted and flushed'
			source.accept(2)
			source.flush()

		then:
			'the updated reduction is available'
			value.get() == 2

		when:
			'use an initial value'
			value = source.compose().reduce(new Reduction(), 2).tap()
			source.accept(1)
			source.flush()

		then:
			'the updated reduction is available'
			value.get() == 2
	}

	def 'When an unknown number of values is being scanned, each reduction is passed to a consumer'() {
		given:
			'a composable with a reduce function'
			Deferred<Integer, Stream<Integer>> source = Streams.<Integer> defer().synchronousDispatcher().get()
			Stream reduced = source.compose().scan(new Reduction())
			def value = reduced.tap()

		when:
			'the first value is accepted'
			source.accept(1)

		then:
			'the reduction is available'
			value.get() == 1

		when:
			'the second value is accepted'
			source.accept(2)

		then:
			'the updated reduction is available'
			value.get() == 2

		when:
			'use an initial value'
			value = source.compose().scan(new Reduction(), 4).tap()
			source.accept(1)
			source.flush()

		then:
			'the updated reduction is available'
			value.get() == 4
	}



	def 'Reduce will accumulate a list of accepted values'() {
		given:
			'a composable'
			Deferred source = Streams.defer().batchSize(1).synchronousDispatcher().get()
			Stream reduced = source.compose().collect()
			def value = reduced.tap()

		when:
			'the first value is accepted'
			source.accept(1)

		then:
			'the list contains the first element'
			value.get() == [1]
	}

	def 'Buffer will accumulate a list of accepted values and pass it to a consumer one by one'() {
		given:
			'a source and a collected stream'
			Deferred source = Streams.<Integer>defer().synchronousDispatcher().get()
			Stream reduced = source.compose().buffer()
			def value = reduced.tap()

		when:
			'the first value is accepted on the source'
			source.accept(1)

		then:
			'the collected list is not yet available'
			value.get() == null

		when:
			'the second value is accepted'
			source.accept(2)
		  reduced.flush()

		then:
			'the tapped value contains the last element'
			value.get() == 2
	}

	def 'BufferWithErrors will accumulate a list of accepted values or errors and pass it to a consumer one by one'() {
		given:
			'a source and a collected stream'
			Deferred source = Streams.<Integer>defer().synchronousDispatcher().get()
			Stream reduced = source.compose().bufferWithErrors()

		  def error = 0
			def value = reduced.when(Exception, consumer{error++}).tap()

		when:
			'the first value is accepted on the source'
			source.accept(1)

		then:
			'the collected list is not yet available'
			value.get() == null

		when:
			'the second value is accepted'
			source.accept(new Exception())
		  reduced.flush()

		then:
			'the error consumer has been invoked and the tapped value is 1'
			error == 1
			value.get() == 1
	}

	def 'Collect will accumulate a list of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			Deferred source = Streams.defer().batchSize(2).synchronousDispatcher().get()
			Stream reduced = source.compose().collect()
			def value = reduced.tap()

		when:
			'the first value is accepted on the source'
			source.accept(1)

		then:
			'the collected list is not yet available'
			value.get() == null

		when:
			'the second value is accepted'
			source.accept(2)

		then:
			'the collected list contains the first and second elements'
			value.get() == [1, 2]
	}

	def 'Collect will accumulate a list of accepted values until flush and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			Deferred source = Streams.<Integer>defer().synchronousDispatcher().get()
			Stream reduced = source.compose().collect()
			def value = reduced.tap()

		when:
			'the first value is accepted on the source'
			source.accept(1)

		then:
			'the collected list is not yet available'
			value.get() == null

		when:
			'the second value is accepted'
			source.accept(2)
			source.flush()

		then:
			'the collected list contains the first and second elements'
			value.get() == [1, 2]
	}

	def 'Creating Stream from environment'() {
		given:
			'a source stream with a given environment'
			Environment environment = new Environment()
			Deferred source = Streams.<Integer> defer(environment, 'ringBuffer')
			Deferred source2 = Streams.<Integer> defer(environment)

		when:
			'accept a value'
			CountDownLatch latch = new CountDownLatch(2)
			def v = ""
			source.compose().consume(consumer { v = 'ok'; latch.countDown() })
			source2.compose().consume(consumer { v = 'ok'; latch.countDown() })
			source.accept(1)
			source2.accept(1)

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
			def r = Reactors.reactor().synchronousDispatcher().get()
			def selector = Selectors.anonymous()
			def event = null
			Streams.<Integer> on(r, selector).consumeEvent(consumer { event = it })

		when:
			'accept a value'
			r.notify(selector.object, Event.wrap(1))

		then:
			'dispatching works'
			event
			event.data == 1
	}

	def 'Window will accumulate a list of accepted values and pass it to a consumer on the specified period'() {
		given:
			'a source and a collected stream'
			Environment environment = new Environment()
			Deferred source = Streams.<Integer> defer().synchronousDispatcher().env(environment).get()
			Stream reduced = source.compose().window(500)
			def value = reduced.tap()

		when:
			'the first values are accepted on the source'
			source.accept(1)
			source.accept(2)
			sleep(1000)

		then:
			'the collected list is not yet available'
			value.get() == [1, 2]

		when:
			'the second value is accepted'
			source.accept(3)
			source.accept(4)
			sleep(1000)

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
			Deferred source = Streams.<Integer> defer().synchronousDispatcher().env(environment).get()
			Stream reduced = source.compose().collectWithTimeout(5, 1000)
			def value = reduced.tap()

		when:
			'the first values are accepted on the source'
			source.accept(1)
			source.accept(1)
			source.accept(1)
			source.accept(1)
			source.accept(1)

		then:
			'the collected list is not yet available'
			value.get() == [1, 1, 1, 1, 1]

		when:
			'the second value is accepted'
			source.accept(2)
			source.accept(2)
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
			Deferred source = Streams.<Integer> defer().synchronousDispatcher().env(environment).get()
			Stream reduced = source.compose().movingWindow(500, 5)
			def value = reduced.tap()

		when:
			'the window accepts first items'
			source.accept(1)
			source.accept(2)
			sleep(1000)

		then:
			'it outputs received values'
			value.get() == [1, 2]

		when:
			'the window accepts following items'
			source.accept(3)
			source.accept(4)
			sleep(1000)

		then:
			'it outputs received values'
			value.get() == [1, 2, 3, 4]

		when:
			'the starts dropping items on overflow'
			source.accept(5)
			source.accept(6)
			sleep(1000)

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
			Deferred source = Streams.<Integer> defer().synchronousDispatcher().env(environment).get()
			Stream reduced = source.compose().movingWindow(500, 5)
			def value = reduced.tap()

		when:
			'the window overflows'
			source.accept(1)
			source.accept(2)
			source.accept(3)
			source.accept(4)
			source.accept(5)
			source.accept(6)
			sleep(1000)

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
			Deferred head = Streams.defer().
					env(env).
					batchSize(333).
					dispatcher(Environment.THREAD_POOL).
					get()
			Stream tail = head.compose().collect()
			tail.consume(consumer { List<Integer> ints ->
				println ints.size()
				sum.addAndGet(ints.size())
				latch.countDown()
			})

		when:
			'values are accepted into the head'
			(1..1000).each { head.accept(it) }

		then:
			'results contains the expected values'
			latch.await(5, TimeUnit.SECONDS)
			sum.get() == 999
	}

	def 'An Observable can consume values from a Stream'() {
		given:
			'a Stream and a Observable consumer'
			Deferred d = Streams.defer().synchronousDispatcher().get()
			Stream composable = d.compose()
			Observable observable = Mock(Observable)
			composable.consume('key', observable)

		when:
			'the composable accepts a value'
			d.accept(1)

		then:
			'the observable is notified'
			1 * observable.notify('key', _)
	}

	def 'An observable can consume values from a Stream with a known set of values'() {
		given:
			'a Stream with 3 values'
			Stream stream = Streams.defer([1, 2, 3]).synchronousDispatcher().get()
			Observable observable = Mock(Observable)

		when:
			'a stream consumer is registerd'
			stream.consume('key', observable)

			stream.flush()

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
