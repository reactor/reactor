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
package reactor.rx

import com.fasterxml.jackson.databind.ObjectMapper
import org.reactivestreams.Subscription
import reactor.core.Environment
import reactor.core.dispatch.SynchronousDispatcher
import reactor.event.Event
import reactor.event.EventBus
import reactor.event.Observable
import reactor.event.selector.Selectors
import reactor.function.Function
import reactor.tuple.Tuple2
import spock.lang.Specification

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

class StreamsSpec extends Specification {

	void setup() {
		Environment.initializeIfEmpty().assignErrorJournal()
	}

	def cleanup() {
		Environment.terminate()
	}

	def 'A deferred Stream with an initial value makes that value available immediately'() {
		given:
			'a composable with an initial value'
			Stream stream = Streams.just('test')

		when:
			'the value is retrieved'
			def value = stream.tap()

		then:
			'it is available'
			value.get() == 'test'
	}

	def 'A deferred Stream with an initial value makes that value available once if broadcasted'() {
		given:
			'a composable with an initial value'
			def stream = Streams.just('test').broadcast()

		when:
			'the value is retrieved'
			def value = stream.tap()
			def value2 = stream.tap()

		then:
			'it is available in value 1 but value 2 has subscribed after dispatching'
			value.get() == 'test'
			!value2.get()
	}

	def 'A Stream can propagate the error using await'() {
		given:
			'a composable with no initial value'
			def stream = Streams.broadcast(Environment.get())

		when:
			'the exception is retrieved after 2 sec'
			Streams.await(stream.timeout(2, TimeUnit.SECONDS))

		then:
			'an exception has been thrown'
			thrown TimeoutException
	}

	def 'A deferred Stream with an initial value makes that value available later'() {
		given:
			'a composable with an initial value'
			Stream stream = Streams.just('test')

		when:
			'the value is not retrieved'
			def value = ""
			def controls = stream.observe { value = it }.consumeLater()

		then:
			'it is not available'
			!value

		when:
			'the value is retrieved'
			controls.start()

		then:
			'it is not available'
			value == 'test'
	}

	def 'A deferred Stream with an initial value makes that value available later up to Long.MAX '() {
		given:
			'a composable with an initial value'
			def e = null
			def latch = new CountDownLatch(1)
			def stream = Streams.from([1, 2, 3])
					.broadcastOn(Environment.masterDispatcher())
					.when(Throwable) { e = it }
					.observeComplete { latch.countDown() }

		when:
			'cumulated request of Long MAX'
			long test = Long.MAX_VALUE / 2l
			def controls = stream.consumeLater()
			controls.requestMore(test)
			controls.requestMore(test)
			controls.requestMore(1)

			//sleep(2000)

		then:
			'no error available'
			latch.await(2, TimeUnit.SECONDS)
			!e
	}

	def 'A deferred Stream with initial values can be consumed multiple times'() {
		given:
			'a composable with an initial value'
			def stream = Streams.just('test', 'test2', 'test3').map { it }.log()

		when:
			'the value is retrieved'
			def value1 = stream.toList().await()
			def value2 = stream.toList().await()

		then:
			'it is available'
			value1 == value2
	}


	def 'A deferred Stream can listen for terminal states'() {
		given:
			'a composable with an initial value'
			def stream = Streams.just('test')

		when:
			'the complete signal is observed and stream is retrieved'
			def value = null

			def tap = stream.finallyDo {
				println 'test'
				value = it
			}.tap()

			println tap.debug()

		then:
			'it is available'
			value == stream
	}

	def 'A deferred Stream can be run on various dispatchers'() {
		given:
			'a composable with an initial value'
			def dispatcher1 = new SynchronousDispatcher()
			def dispatcher2 = new SynchronousDispatcher()
			def stream = Streams.just('test', 'test2', 'test3').dispatchOn(dispatcher1)
			def tail = stream.observe {}

		when:
			'the stream is retrieved'
			tail = tail.dispatchOn(dispatcher2)

		then:
			'it is available'
			stream.dispatcher == dispatcher1
			tail.dispatcher == dispatcher2
	}

	def 'A deferred Stream can be translated into a list'() {
		given:
			'a composable with an initial value'
			Stream stream = Streams.just('test', 'test2', 'test3')

		when:
			'the stream is retrieved'
			def value = stream.map { it + '-ok' }.toList()

			println value.debug()

		then:
			'it is available'
			value.get() == ['test-ok', 'test2-ok', 'test3-ok']
	}

	def 'A deferred Stream can be translated into a completable queue'() {
		given:
			'a composable with an initial value'
			def stream = Streams.just('test', 'test2', 'test3').dispatchOn(Environment.get())

		when:
			'the stream is retrieved'
			stream = stream.map { it + '-ok' }.log()

			def queue = stream.toBlockingQueue()
			//	println stream.debug()

			def res
			def result = []

			for (; ;) {
				res = queue.poll()

				if (res)
					result << res

				if (queue.isComplete() && !queue.peek())
					break
			}

		then:
			'it is available'
			result == ['test-ok', 'test2-ok', 'test3-ok']
	}

	def 'A deferred Stream with a generated value makes that value available immediately'() {
		given:
			String test = ""
			'a composable with an initial value'
			def stream = Streams.<String> generate { test }

		when:
			'the value is retrieved'
			test = "test"
			def value = stream.tap()

		then:
			'it is available'
			value.get() == 'test'

		when:
			'nothing is provided'
			Streams.generate(null)

		then:
			"exception is thrown"
			thrown(IllegalArgumentException)

			/*when:
				'something is provided and requested 20 elements'
				def i = 0
				Streams.generate{ i++ }.consume(20)

			then:
				"20 have been generated"
				i == 20
			when:
				'something is provided and requested 20 elements and cancelled at 10'
				i = 0
				Streams.generate{ i++ }.subscribe(new Subscriber<Integer>() {
					def s
					@Override
					void onSubscribe(Subscription subscription) {
						s = subscription
						s.request(20)
					}

					@Override
					void onNext(Integer integer) {
						if(integer == 9){
							s.cancel()
						}
					}

					@Override
					void onError(Throwable throwable) {
						throwable.printStackTrace()
					}

					@Override
					void onComplete() {
						println 'complete'
					}
				})

			then:
				"10 have been generated"
				i == 10*/
	}

	def 'A Stream with a known set of values makes those values available immediately'() {
		given:
			'a composable with values 1 to 5 inclusive'
			Stream s = Streams.from([1, 2, 3, 4, 5])

		when:
			'the first value is retrieved'
			def first = s.sampleFirst().tap()

		and:
			'the last value is retrieved'
			def last = s.sample().tap()

		then:
			'first and last'
			first.get() == 1
			last.get() == 5
	}

	def 'A Stream can sample values over time'() {
		given:
			'a composable with values 1 to INT_MAX inclusive'
			def s = Streams.range(1, Integer.MAX_VALUE)
					.requestOn(Environment.get())

		when:
			'the most recent value is retrieved'
			def last = s
					.sample(2l, TimeUnit.SECONDS)
					.dispatchOn(Environment.cachedDispatcher())
					.log()
					.next()

		then:
			last.await(5, TimeUnit.SECONDS) > 20_000

		cleanup:
			println last.debug()
	}

	def 'A Stream can be enforced to dispatch distinct values'() {
		given:
			'a composable with values 1 to 3 with duplicates'
			Stream s = Streams.from([1, 1, 2, 2, 3])

		when:
			'the values are filtered and result is collected'
			def tap = s.distinctUntilChanged().buffer().tap()

		then:
			'collected must remove duplicates'
			tap.get() == [1, 2, 3]
	}

	def "A Stream's initial values are passed to consumers"() {
		given:
			'a composable with values 1 to 5 inclusive'
			Stream stream = Streams.from([1, 2, 3, 4, 5])

		when:
			'a Consumer is registered'
			def values = []
			stream.consume { values << it }

		then:
			'the initial values are passed'
			values == [1, 2, 3, 4, 5]


	}

	def "Stream 'state' related signals can be consumed"() {
		given:
			'a composable with values 1 to 5 inclusive'
			def stream = Streams.from([1, 2, 3, 4, 5])
			def values = []
			def signals = []

		when:
			'a Subscribe Consumer is registered'
			stream = stream.observeSubscribe { signals << 'subscribe' }

		and:
			'a Cancel Consumer is registered'
			stream = stream.observeCancel { signals << 'cancel' }

		and:
			'a Complete Consumer is registered'
			stream = stream.observeComplete { signals << 'complete' }

		and:
			'the stream is consumed'
			stream.consume { values << it }

		then:
			'the initial values are passed'
			values == [1, 2, 3, 4, 5]
			'subscribe' == signals[0]
			'complete' == signals[1]
			'cancel' == signals[2]
	}

	def "Stream can emit a default value if empty"() {
		given:
			'a composable that only completes'
			def stream = Streams.<String> empty()
			def values = []

		when:
			'a Subscribe Consumer is registered'
			stream = stream.defaultIfEmpty('test').observeComplete { values << 'complete' }

		and:
			'the stream is consumed'
			stream.consume { values << it }

		then:
			'the initial values are passed'
			values == ['test', 'complete']
	}

	def 'Accepted values are passed to a registered Consumer'() {
		given:
			'a composable with a registered consumer'
			def composable = Streams.<Integer> broadcast()
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
			Stream composable = Streams.<Integer> broadcast()
			def errors = 0
			def tail = composable.when(RuntimeException) { errors++ }.consume()
			println tail.debug()

		when:
			'A RuntimeException is accepted'
			composable.broadcastError(new RuntimeException())

		then:
			'it is passed to the consumer'
			errors == 1

		when:
			'A new error consumer is subscribed'
			composable.when(RuntimeException) { errors++ }.consume()

		then:
			'it is called since publisher is in error state'
			errors == 2

		when:
			'A RuntimeException is accepted'
			composable = Streams.<Integer> broadcast()
			composable.broadcastError(new IllegalArgumentException())

		then:
			'it is not passed to the consumer'
			errors == 2
	}

	def 'When the accepted event is Iterable, split can iterate over values'() {
		given:
			'a composable with a known number of values'
			def d = Streams.<Iterable<String>> broadcast()
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
			def d = Streams.<Integer> broadcast().capacity(3)
			def composable = d

		when:
			'the expected accept count is set and that number of values is accepted'
			def tap = composable.sample().log().tap()
			println composable.debug()
			d.broadcastNext(1)
			d.broadcastNext(2)
			d.broadcastNext(3)

		then:
			"last's value is now that of the last value"
			tap.get() == 3

		when:
			'the expected accept count is set and that number of values is accepted'
			tap = composable.sample(3).tap()
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
			def source = Streams.<Integer> broadcast()
			Stream mapped = source.map { it * 2 }

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
			def source = Streams.<Integer> broadcast(Environment.get())
			Stream<Integer> mapped = source.
					flatMap { v -> Streams.just(v * 2) }.
					when(Throwable) { it.printStackTrace() }


		when:
			'the source accepts a value'
			def value = mapped.next()
			println source.debug()
			source.broadcastNext(1)
			println source.debug()

		then:
			'the value is mapped'
			value.await() == 2
	}

	def "Multiple Stream's values can be merged"() {
		given:
			'source composables to merge, buffer and tap'
			def source1 = Streams.<Integer> broadcast()

			def source2 = Streams.<Integer> broadcast()
			source2.map { it }.map { it }

			def source3 = Streams.<Integer> broadcast()

			def tap = Streams.merge(source1, source2, source3).log().buffer(3).log().tap()

		when:
			'the sources accept a value'
			source1.broadcastNext(1)
			source2.broadcastNext(2)
			source3.broadcastNext(3)

			println source1.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == [1, 2, 3]
	}


	def "Multiple Stream's values can be joined"() {
		given:
			'source composables to merge, buffer and tap'
			def source2 = Streams.<Integer> broadcast()
			def source3 = Streams.<Integer> broadcast()
			def source1 = Streams.<Stream<Integer>> just(source2, source3)
			def tail = source1.join().log()
			def tap = tail.tap()

			println tap.debug()

		when:
			'the sources accept a value'
			source2.broadcastNext(1)

			println tap.debug()
			source3.broadcastNext(2)
			source3.broadcastNext(3)
			source3.broadcastNext(4)

			println tap.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == [1, 2]

		when:
			'the sources accept the missing value'
			source3.broadcastNext(5)
			source2.broadcastNext(6)

			println tap.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == [6, 3]
	}

	def "Inline Stream's values can be zipped"() {
		given:
			'source composables to merge, buffer and tap'
			def source2 = Streams.<Integer> broadcast()
			def source3 = Streams.<Integer> broadcast()
			def source1 = Streams.<Stream<Integer>> just(source2, source3)
			def tail = source1.zip { it.t1 + it.t2 }.log().when(Throwable) { it.printStackTrace() }

			def tap = tail.tap()
			println tap.debug()

		when:
			'the sources accept a value'
			source2.broadcastNext(1)
			source3.broadcastNext(2)
			source3.broadcastNext(3)
			source3.broadcastNext(4)

			println tap.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == 3

		when:
			'the sources accept the missing value'
			source3.broadcastNext(5)
			source2.broadcastNext(6)

			println tap.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == 9
	}


	def "Multiple Stream's values can be zipped"() {
		given:
			'source composables to merge, buffer and tap'
			def source1 = Streams.<Integer> broadcast()
			def source2 = Streams.<Integer> broadcast()
			def zippedStream = Streams.zip(source1, source2) { println it; it.t1 + it.t2 }.log()
			def tap = zippedStream.tap()

		when:
			'the sources accept a value'
			source1.broadcastNext(1)
			source2.broadcastNext(2)
			source2.broadcastNext(3)
			source2.broadcastNext(4)

			println tap.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == 3

		when:
			'the sources accept the missing value'
			source2.broadcastNext(5)
			source1.broadcastNext(6)

			println tap.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == 9
	}

	def "Multiple iterable Stream's values can be zipped"() {
		given:
			'source composables to zip, buffer and tap'
			def odds = Streams.just(1, 3, 5, 7, 9)
			def even = Streams.just(2, 4, 6)

		when:
			'the sources are zipped'
			def zippedStream = Streams.zip(odds, even) { [it.t1, it.t2] }
			def tap = zippedStream.log().toList()
			tap.await(3, TimeUnit.SECONDS)
			println tap.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == [[1, 2], [3, 4], [5, 6]]

		when:
			'the sources are zipped in a flat map'
			zippedStream = odds.log('before-flatmap').flatMap {
				Streams.zip(Streams.just(it), even) { [it.t1, it.t2] }
						.log('second-fm')
			}
			tap = zippedStream.log('after-zip').toList()
			tap.await(3, TimeUnit.SECONDS)
			println tap.debug()


		then:
			'the values are all collected from source1 stream'
			tap.get() == [[1, 2], [3, 2], [5, 2], [7, 2], [9, 2]]
	}


	def "A different way of consuming"() {
		given:
			'source composables to zip, buffer and tap'
			def odds = Streams.just(1, 3, 5, 7, 9)
			def even = Streams.just(2, 4, 6)

		when:
			'the sources are zipped'
			def mergedStream = Streams.merge(odds, even)
			def res = []
			mergedStream.consume(
					{ res << it; println it },
					{ it.printStackTrace() },
					{ res.sort(); res << 'done'; println 'completed!' }
			)

			println mergedStream.debug()

		then:
			'the values are all collected from source1 and source2 stream'
			res == [1, 2, 3, 4, 5, 6, 7, 9, 'done']

	}

	def "A simple concat"() {
		given:
			'source composables to zip, buffer and tap'
			def firsts = Streams.just(1, 2, 3)
			def lasts = Streams.just(4, 5)

		when:
			'the sources are zipped'
			def mergedStream = Streams.concat(firsts, lasts)
			def res = []
			mergedStream.consume(
					{ res << it; println it },
					{ it.printStackTrace() },
					{ res << 'done'; println 'completed!' }
			)

			println mergedStream.debug()

		then:
			'the values are all collected from source1 and source2 stream'
			res == [1, 2, 3, 4, 5, 'done']
	}

	def "A mapped concat"() {
		given:
			'source composables to zip, buffer and tap'
			def firsts = Streams.just(1, 2, 3)

		when:
			'the sources are zipped'
			def mergedStream = firsts.concatMap { Streams.range(it, 3) }
			def res = []
			println mergedStream.consume(
					{ res << it; println it },
					{ it.printStackTrace() },
					{ res << 'done'; println 'completed!' }
			).debug()

		then:
			'the values are all collected from source1 and source2 stream'
			res == [1, 2, 3, 2, 3, 3, 'done']
	}

	def "Stream can be counted"() {
		given:
			'source composables to count and tap'
			def source = Streams.<Integer> broadcast()
			def tap = source.count(3).tap()

		when:
			'the sources accept a value'
			source.broadcastNext(1)
			source.broadcastNext(2)
			source.broadcastNext(3)

		then:
			'the count value matches the number of accept'
			tap.get() == 3
	}

	def "A Stream's values can be filtered"() {
		given:
			'a source composable with a filter that rejects odd values'
			def source = Streams.<Integer> broadcast()
			Stream filtered = source.filter { it % 2 == 0 }

		when:
			'the source accepts an even value'
			def value = filtered.tap()
			println value.debug()
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
			'simple filter'
			def anotherSource = Streams.<Boolean> broadcast()
			def tap = anotherSource.filter().tap()
			anotherSource.broadcastNext(true)

		then:
			'it is accepted by the filter'
			tap.get()

		when:
			'simple filter nominal case'
			anotherSource = Streams.<Boolean> broadcast()
			tap = anotherSource.filter().tap()
			anotherSource.broadcastNext(false)

		then:
			'it is not accepted by the filter'
			!tap.get()
	}

	def "When a mapping function throws an exception, the mapped composable accepts the error"() {
		given:
			'a source composable with a mapping function that throws an exception'
			def source = Streams.<Integer> broadcast()
			Stream mapped = source.map { if (it == 1) throw new RuntimeException() else 'na' }
			def errors = 0
			mapped.when(Exception) { errors++ }.consume()

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
			def source = Streams.<Integer> broadcast()
			Stream filtered = source.filter { if (it == 1) throw new RuntimeException() else true }
			def errors = 0
			filtered.when(Exception) { errors++ }.consume()

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
			Stream source = Streams.from([1, 2, 3, 4, 5])

		when:
			'a reduce function is registered'
			def reduced = source.reduce(new Reduction())
			def value = reduced.tap()

		then:
			'the resulting composable holds the reduced value'
			value.get() == 120

		when:
			'use an initial value'
			value = source.reduce(2, new Reduction()).tap()
			println value.debug()

		then:
			'the updated reduction is available'
			value.get() == 240
	}

	def "When reducing a known set of values, only the final value is passed to consumers"() {
		given:
			'a composable with a known set of values and a reduce function'
			def reduced = Streams.<Integer> just(1, 2, 3, 4, 5)
					.reduce(new Reduction())

		when:
			'a consumer is registered'
			def values = []
			reduced.consume { values << it }

		then:
			'the consumer only receives the final value'
			values == [120]
	}

	def "When reducing a known number of values, only the final value is passed to consumers"() {
		given:
			'a composable with a known number of values and a reduce function'
			def source = Streams.<Integer> broadcast().capacity(5)
			Stream reduced = source.reduce(new Reduction())
			def values = []
			reduced.consume { values << it }

		when:
			'the expected number of values is accepted'
			source.broadcastNext(1)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastNext(4)
			source.broadcastNext(5)
			println source.debug()
		then:
			'the consumer only receives the final value'
			values == [120]
	}

	def 'A known number of values can be reduced'() {
		given:
			'a composable that will accept 5 values and a reduce function'
			def source = Streams.<Integer> broadcast().capacity(5)
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
			def source = Streams.<Integer> broadcast().capacity(2)
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
			def source = Streams.<Integer> broadcast()
			def reduced = source.window(2).log().flatMap { it.log('lol').reduce(new Reduction()) }
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

		then:
			'the updated reduction is available'
			value.get() == 2
	}

	def 'When an unknown number of values is being scanned, each reduction is passed to a consumer'() {
		given:
			'a composable with a reduce function'
			def source = Streams.<Integer> broadcast()
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
			value = source.scan(4, new Reduction()).tap()
			source.broadcastNext(1)

		then:
			'the updated reduction is available'
			value.get() == 4
	}


	def 'Reduce will accumulate a list of accepted values'() {
		given:
			'a composable'
			def source = Streams.<Integer> broadcast().capacity(1)
			Stream reduced = source.buffer()
			def value = reduced.tap()

		when:
			'the first value is accepted'
			println value.debug()
			source.broadcastNext(1)
			println value.debug()

		then:
			'the list contains the first element'
			value.get() == [1]
	}

	def 'Collect will accumulate a list of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer> broadcast().capacity(2)
			Stream reduced = source.buffer()
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


	def 'Collect will accumulate multiple lists of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			def numbers = Streams.from([1, 2, 3, 4, 5, 6, 7, 8]);

		when:
			'non overlapping buffers'
			def res = numbers.buffer(2, 3).toList()

		then:
			'the collected lists are available'
			res.await() == [[1, 2], [4, 5], [7, 8]]

		when:
			'overlapping buffers'
			res = numbers.buffer(3, 2).toList()

		then:
			'the collected overlapping lists are available'
			res.await() == [[1, 2, 3], [3, 4, 5], [5, 6, 7], [7, 8]]


		when:
			'non overlapping buffers'
			res = numbers.throttle(200).buffer(400l, 600l, TimeUnit.MILLISECONDS).toList()

		then:
			'the collected lists are available'
			res.await() == [[1, 2], [4, 5], [7, 8]]
	}


	def 'Collect will accumulate multiple lists of accepted values and pass it to a consumer on bucket close'() {
		given:
			'a source and a collected stream'
			def numbers = Streams.<Integer> broadcast();

		when:
			'non overlapping buffers'
			def boundaryStream = Streams.<Integer> broadcast()
			def res = numbers.buffer{ boundaryStream }.toList()

			numbers.broadcastNext(1)
			numbers.broadcastNext(2)
			numbers.broadcastNext(3)
			boundaryStream.broadcastNext(1)
			numbers.broadcastNext(5)
			numbers.broadcastNext(6)
			numbers.broadcastComplete()

		then:
			'the collected lists are available'
			res.await() == [[1, 2, 3], [5, 6]]

		when:
			'overlapping buffers'
			def bucketOpening = Streams.<Integer> broadcast()
			res = numbers.buffer(bucketOpening){boundaryStream}.toList()

			numbers.broadcastNext(1)
			numbers.broadcastNext(2)
			bucketOpening.broadcastNext(1)
			numbers.broadcastNext(3)
			bucketOpening.broadcastNext(1)
			numbers.broadcastNext(5)
			boundaryStream.broadcastComplete()
			bucketOpening.broadcastNext(1)
			numbers.broadcastNext(6)
			bucketOpening.broadcastComplete()


		then:
			'the collected overlapping lists are available'
			res.await() == [[3, 5], [5], [6]]
	}

	def 'Window will reroute multiple stream of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			def numbers = Streams.from([1, 2, 3, 4, 5, 6, 7, 8])

		when:
			'non overlapping buffers'
			def res = numbers.window(2, 3).flatMap{it.buffer()}.toList()

		then:
			'the collected lists are available'
			res.await() == [[1, 2], [4, 5], [7, 8]]

		when:
			'overlapping buffers'
			res = numbers.window(3, 2).flatMap{it.buffer()}.toList()

		then:
			'the collected overlapping lists are available'
			res.await() == [[1, 2, 3], [3, 4, 5], [5, 6, 7], [7, 8]]


		when:
			'non overlapping buffers'
			res = numbers.throttle(200).window(400l, 600l, TimeUnit.MILLISECONDS).flatMap{it.log('fm').buffer()}.toList()

		then:
			'the collected lists are available'
			res.await() == [[1, 2], [4, 5], [7, 8]]

	}


	def 'Re route will accumulate multiple lists of accepted values and pass it to a consumer on bucket close'() {
		given:
			'a source and a collected stream'
			def numbers = Streams.<Integer> broadcast();

		when:
			'non overlapping buffers'
			def boundaryStream = Streams.<Integer> broadcast()
			def res = numbers.window{ boundaryStream }.flatMap{it.buffer()}.toList()

			numbers.broadcastNext(1)
			numbers.broadcastNext(2)
			numbers.broadcastNext(3)
			boundaryStream.broadcastNext(1)
			numbers.broadcastNext(5)
			numbers.broadcastNext(6)
			numbers.broadcastComplete()

		then:
			'the collected lists are available'
			res.await() == [[1, 2, 3], [5, 6]]

		when:
			'overlapping buffers'
			def bucketOpening = Streams.<Integer> broadcast()
			res = numbers.window(bucketOpening){boundaryStream}.flatMap{it.buffer()}.toList()

			numbers.broadcastNext(1)
			numbers.broadcastNext(2)
			bucketOpening.broadcastNext(1)
			numbers.broadcastNext(3)
			bucketOpening.broadcastNext(1)
			numbers.broadcastNext(5)
			boundaryStream.broadcastComplete()
			bucketOpening.broadcastNext(1)
			numbers.broadcastNext(6)
			bucketOpening.broadcastComplete()


		then:
			'the collected overlapping lists are available'
			res.await() == [[3, 5], [5], [6]]
	}

	def 'Window will re-route N elements to a fresh nested stream'() {
		given:
			'a source and a collected window stream'
			def source = Streams.<Integer> broadcast().capacity(2)
			def value = null

			source.log('w').window().consume {
				value = it.log().buffer(2).tap()
			}


		when:
			'the first value is accepted on the source'
			source.broadcastNext(1)

		then:
			'the collected list is not yet available'
			value
			value.get() == null

		when:
			'the second value is accepted'
			source.broadcastNext(2)
			println value.debug()

		then:
			'the collected list contains the first and second elements'
			value.get() == [1, 2]

		when:
			'2 more values are accepted'
			source.broadcastNext(3)
			source.broadcastNext(4)
			println value.debug()

		then:
			'the collected list contains the first and second elements'
			value.get() == [3, 4]
	}

	def 'Window will re-route N elements over time to a fresh nested stream'() {
		given:
			'a source and a collected window stream'
			def source = Streams.<Integer> broadcast(Environment.get())
			def promise = Promises.prepare()

			source.log("prewindow").window(10l, TimeUnit.SECONDS).consume {
				it.log().buffer(2).consume { promise.onNext(it) }
			}


		when:
			'the first value is accepted on the source'
			source.broadcastNext(1)
			println source.debug()

		then:
			'the collected list is not yet available'
			promise.get() == null

		when:
			'the second value is accepted'
			source.broadcastNext(2)
			println source.debug()

		then:
			'the collected list contains the first and second elements'
			promise.await() == [1, 2]

		when:
			'2 more values are accepted'
			promise = Promises.prepare()

			sleep(2000)
			source.broadcastNext(3)
			source.broadcastNext(4)
			println source.debug()

		then:
			'the collected list contains the first and second elements'
			promise.await() == [3, 4]
	}

	def 'GroupBy will re-route N elements to a nested stream based on the mapped key'() {
		given:
			'a source and a grouped by ID stream'
			def source = Streams.<SimplePojo> broadcast()
			def result = [:]

			source.groupBy { pojo ->
				pojo.id
			}.consume { stream ->
				stream.consume { pojo ->
					if (result[pojo.id]) {
						result[pojo.id] << pojo.title
					} else {
						result[pojo.id] = [pojo.title]
					}
				}
			}

			def objetMapper = new ObjectMapper()
			println objetMapper.writeValueAsString(source.debug().toMap())

		when:
			'some values are accepted'
			source.broadcastNext(new SimplePojo(id: 1, title: 'Stephane'))
			source.broadcastNext(new SimplePojo(id: 1, title: 'Jon'))
			source.broadcastNext(new SimplePojo(id: 1, title: 'Sandrine'))
			source.broadcastNext(new SimplePojo(id: 2, title: 'Acme'))
			source.broadcastNext(new SimplePojo(id: 3, title: 'Acme2'))
			source.broadcastNext(new SimplePojo(id: 3, title: 'Acme3'))
			source.broadcastComplete()

		then:
			'the result should group titles by id'
			result
			result == [
					1: ['Stephane', 'Jon', 'Sandrine'],
					2: ['Acme'],
					3: ['Acme2', 'Acme3']
			]
	}

	def 'GroupBy will re-route N elements to a nested stream based on hashcode'() {
		given:
			'a source and a grouped by ID stream'
			def source = Streams.<SimplePojo> broadcast(Environment.get())
			def result = [:]
			def latch = new CountDownLatch(6)

			def partitionStream = source.partition()
			partitionStream.consume { stream ->
				stream.cast(SimplePojo).consume { pojo ->
					if (result[pojo.id]) {
						result[pojo.id] << pojo.title
					} else {
						result[pojo.id] = [pojo.title]
					}
					latch.countDown()
				}
			}

			def objetMapper = new ObjectMapper()
			println objetMapper.writeValueAsString(source.debug().toMap())

		when:
			'some values are accepted'
			source.broadcastNext(new SimplePojo(id: 1, title: 'Stephane'))
			source.broadcastNext(new SimplePojo(id: 1, title: 'Jon'))
			source.broadcastNext(new SimplePojo(id: 1, title: 'Sandrine'))
			source.broadcastNext(new SimplePojo(id: 2, title: 'Acme'))
			source.broadcastNext(new SimplePojo(id: 3, title: 'Acme2'))
			source.broadcastNext(new SimplePojo(id: 3, title: 'Acme3'))


		then:
			'the result should group titles by id'
			latch.await(5, TimeUnit.SECONDS)
			result
			result == [
					1: ['Stephane', 'Jon', 'Sandrine'],
					2: ['Acme'],
					3: ['Acme2', 'Acme3']
			]
	}

	def 'StreamUtils will parse a Stream to a Map'() {
		given:
			'a source and a grouped by ID stream'
			def source = Streams.<SimplePojo> broadcast()

			source.groupBy { pojo ->
				pojo.id
			}.consume { stream ->
				stream.consume { pojo ->

				}
			}

		when:
			'some values are accepted'
			source.broadcastNext(new SimplePojo(id: 1, title: 'Stephane'))
			source.broadcastNext(new SimplePojo(id: 1, title: 'Jon'))
			source.broadcastNext(new SimplePojo(id: 1, title: 'Sandrine'))
			source.broadcastNext(new SimplePojo(id: 2, title: 'Acme'))
			source.broadcastNext(new SimplePojo(id: 3, title: 'Acme2'))
			source.broadcastNext(new SimplePojo(id: 3, title: 'Acme3'))
			println source.debug()
			def result = source.debug().toMap()


		then:
			'the result should contain all stream titles by id'
			result.to[0].id == "GroupBy"
			result.to[0].to[0].id == "TerminalCallback"
			result.to[0].boundTo[0].id == "TerminalCallback"
			result.to[0].boundTo[1].id == "TerminalCallback"
			result.to[0].boundTo[2].id == "TerminalCallback"

		when: "complete will cancel non kept-alive actions"
			source.broadcastComplete()
			result = source.debug().toMap()
			println source.debug()

		then:
			'the result should contain zero stream'
			!result.to
	}

	def 'Creating Stream from Environment.get()'() {
		given:
			'a source stream with a given environment'
			def source = Streams.<Integer> broadcast(Environment.get(), Environment.dispatcher('ringBuffer'))
			def source2 = Streams.<Integer> broadcast(Environment.get())

		when:
			'accept a value'
			CountDownLatch latch = new CountDownLatch(2)
			def v = ""
			source.consume { v = 'ok'; latch.countDown() }
			source2.consume { v = 'ok'; latch.countDown() }
			source.broadcastNext(1)
			source2.broadcastNext(1)

		then:
			'dispatching works'
			latch.await(1000, TimeUnit.MILLISECONDS)
			v == 'ok'
	}

	def 'Creating Stream from observable'() {
		given:
			'a source stream with a given observable'
			def r = EventBus.config().get()
			def selector = Selectors.anonymous()
			int event = 0
			def s = Streams.<Integer> on(r, selector).consume { event = it }
			println s.debug()

		when:
			'accept a value'
			r.notify(selector.object, Event.wrap(1))
			println s.debug()

		then:
			'dispatching works'
			event == 1
	}

	def 'Creating Stream from publisher'() {
		given:
			'a source stream with a given publisher'
			def s = Streams.<String> create {
				it.onNext('test1')
				it.onNext('test2')
				it.onNext('test3')
				it.onComplete()
			}.log()

		when:
			'accept a value'
			def result = s.toList().await(5, TimeUnit.SECONDS)
			//println s.debug()

		then:
			'dispatching works'
			result == ['test1', 'test2', 'test3']
	}

	def 'Creating Stream from publisher factory'() {
		given:
			'a source stream with a given publisher factory'
			def i = 0
			def res = []
			def s = Streams.<Integer> defer {
				Streams.just(i++)
			}.log()

		when:
			'accept a value'
			res << s.next().await(5, TimeUnit.SECONDS)
			res << s.next().await(5, TimeUnit.SECONDS)
			res << s.next().await(5, TimeUnit.SECONDS)
			//println s.debug()

		then:
			'dispatching works'
			res == [0, 1, 2]
	}

	def 'Wrap Stream from publisher'() {
		given:
			'a source stream with a given publisher factory'
			def terminated = false
			def s = Streams.<Integer> wrap {
				it.onSubscribe(new Subscription() {
					@Override
					void request(long n) {
						it.onNext(1)
						it.onNext(2)
						it.onNext(3)
						it.onComplete()
					}

					@Override
					void cancel() {
						terminated = true
					}
				})
			}.log()

		when:
			'accept a value'
			def res = s.toList().await(5, TimeUnit.SECONDS)
			//println s.debug()

		then:
			'dispatching works'
			res == [1, 2, 3]
			terminated
	}

	def 'Creating Stream from Timer'() {
		given:
			'a source stream with a given timer'

			def res = 0l
			def c = Streams.timer(1)
			def timeStart = System.currentTimeMillis()

		when:
			'consuming'
			c.consume {
				res = System.currentTimeMillis() - timeStart
			}
			sleep(2500)

		then:
			'ready'
			res > 950

		when:
			'consuming periodic'
			def i = []
			Streams.period(0, 1).consume {
				i << it
			}
			sleep(2500)

		then:
			'ready'
			i.containsAll([0l, 1l])
	}


	def 'Creating Stream from range'() {
		given:
			'a source stream with a given range'
			def s = Streams.range(1, 10)

		when:
			'accept a value'
			def result = s.toList().await()

		then:
			'dispatching works'
			result == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	}

	def 'Creating Empty Streams from publisher'() {
		given:
			'a source stream pre-completed'
			def s = Streams.empty()

		when:
			'consume it'
			def latch = new CountDownLatch(1)
			def nexts = []
			def errors = []

			s.consume(
					{ nexts << 'never' },
					{ errors << 'never ever' },
					{ latch.countDown() }
			)

		then:
			'dispatching works'
			latch.await(2, TimeUnit.SECONDS)
			!nexts
			!errors
	}

	def 'Creating Streams from future'() {
		given:
			'a source stream pre-completed'
			def executorService = Executors.newSingleThreadExecutor()

			def future = executorService.submit({
				'hello future'
			} as Callable<String>)

			def s = Streams.from(future)

		when:
			'consume it'
			def latch = new CountDownLatch(1)
			def nexts = []
			def errors = []

			s.consume(
					{ nexts << it },
					{ errors << 'never ever' },
					{ latch.countDown() }
			)

		then:
			'dispatching works'
			latch.await(2, TimeUnit.SECONDS)
			!errors
			nexts[0] == 'hello future'

		when: 'timeout'
			latch = new CountDownLatch(1)

			future = executorService.submit({
				sleep(2000)
				'hello future too long'
			} as Callable<String>)

			s = Streams.from(future, 100, TimeUnit.MILLISECONDS).dispatchOn(Environment.get())
			nexts = []
			errors = []

			s.consume(
					{ nexts << 'never ever' },
					{ errors << it; latch.countDown() }
			)

		then:
			'error dispatching works'
			latch.await(2, TimeUnit.SECONDS)
			errors[0] in TimeoutException
			!nexts

		cleanup:
			executorService.shutdown()
	}

	def 'Throttle will accumulate a list of accepted values and pass it to a consumer on the specified period'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer> broadcast().env(Environment.get())
			def reduced = source.buffer().throttle(300)
			def value = reduced.tap()

		when:
			'the first values are accepted on the source'
			source.broadcastNext(1)
			println source.debug()
			source.broadcastNext(2)
			sleep(1200)
			println source.debug()

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
	}

	def 'Throttle will generate demand every specified period'() {
		given:
			'a source and a collected stream'
			def random = new Random()
			def source = Streams.generate {
				random.nextInt()
			}.dispatchOn(Environment.get())

			def values = []

			source.throttle(200).consume {
				values << it
			}

		when:
			'the first values are accepted on the source'
			sleep(1200)

		then:
			'the collected list is available'
			values

	}

	def 'Collect with Timeout will accumulate a list of accepted values and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer> broadcast().env(Environment.get())
			def reduced = source.buffer(5, 600, TimeUnit.MILLISECONDS)
			def value = reduced.tap()
			println value.debug()

		when:
			'the first values are accepted on the source'
			source.broadcastNext(1)
			source.broadcastNext(1)
			source.broadcastNext(1)
			source.broadcastNext(1)
			source.broadcastNext(1)
			println value.debug()
			sleep(2000)
			println value.debug()

		then:
			'the collected list is not yet available'
			value.get() == [1, 1, 1, 1, 1]

		when:
			'the second value is accepted'
			source.broadcastNext(2)
			source.broadcastNext(2)
			sleep(2000)
			println value.debug()

		then:
			'the collected list contains the first and second elements'
			value.get() == [2, 2]

	}

	def 'Timeout can be bound to a stream'() {
		given:
			'a source and a timeout'
			def source = Streams.<Integer> broadcast().env(Environment.get())
			def reduced = source.timeout(1500, TimeUnit.MILLISECONDS)
			def error = null
			def value = reduced.when(TimeoutException) {
				error = it
			}.tap()
			println value.debug()

		when:
			'the first values are accepted on the source, paused just enough to refresh timer until 6'
			source.broadcastNext(1)
			sleep(500)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastNext(4)
			sleep(500)
			source.broadcastNext(5)
			sleep(2000)
			source.broadcastNext(6)
			println error

		then:
			'last value known is 5 and stream is in error state'
			error in TimeoutException
			value.get() == 5
	}

	def 'Timeout can be bound to a stream and fallback'() {
		given:
			'a source and a timeout'
			def source = Streams.<Integer> broadcast().env(Environment.get())
			def reduced = source.timeout(1500, TimeUnit.MILLISECONDS, Streams.just(10))
			def error = null
			def value = reduced.when(TimeoutException) {
				error = it
			}.tap()
			println value.debug()

		when:
			'the first values are accepted on the source, paused just enough to refresh timer until 6'
			source.broadcastNext(1)
			sleep(500)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastNext(4)
			source.broadcastNext(5)
			sleep(2000)
			source.broadcastNext(6)
			println value.debug()

		then:
			'last value known is 10 as the stream has used its fallback'
			!error
			value.get() == 10
	}

	def 'Errors can have a fallback'() {
		when:
			'A source stream emits next signals followed by an error'
			def res = []
			def myStream = Streams.create { aSubscriber ->
				aSubscriber.onNext('Three')
				aSubscriber.onNext('Two')
				aSubscriber.onNext('One')
				aSubscriber.onError(new Exception())
				aSubscriber.onNext('Zero')
			}

		and:
			'A fallback stream will emit values and complete'
			def myFallback = Streams.create { aSubscriber ->
				aSubscriber.onNext('0')
				aSubscriber.onNext('1')
				aSubscriber.onNext('2')
				aSubscriber.onComplete()
				aSubscriber.onNext('3')
			}

		and:
			'fallback stream is assigned to source stream on any error'
			myStream.onErrorResumeNext(myFallback).consume(
					{ println(it); res << it },                          // onNext
					{ println("Error: " + it.message) }, // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['Three', 'Two', 'One', '0', '1', '2', 'complete']
	}


	def 'Errors can have a fallback return value'() {
		when:
			'A source stream emits next signals followed by an error'
			def res = []
			def myStream = Streams.create { aSubscriber ->
				aSubscriber.onNext('Three')
				aSubscriber.onNext('Two')
				aSubscriber.onNext('One')
				aSubscriber.onError(new Exception())
			}

		and:
			'A fallback value will emit values and complete'
			myStream.onErrorReturn { 'Zero' }.consume(
					{ println(it); res << it },                          // onNext
					{ println("Error: " + it.message) }, // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['Three', 'Two', 'One', 'Zero', 'complete']
	}

	def 'Streams can be materialized'() {
		when:
			'A source stream emits next signals followed by complete'
			def res = []
			def myStream = Streams.create { aSubscriber ->
				aSubscriber.onNext('Three')
				aSubscriber.onNext('Two')
				aSubscriber.onNext('One')
				aSubscriber.onComplete()
				aSubscriber.onError(new Exception())
			}

		and:
			'A materialized stream is consumed'
			myStream.materialize().consume(
					{ println(it); res << it.type.toString() },                          // onNext
					{ it.printStackTrace() },                          // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['SUBSCRIBE', 'NEXT', 'NEXT', 'NEXT', 'COMPLETE', 'complete']

		when:
			'A source stream emits next signals followed by complete'
			res = []
			myStream = Streams.create { aSubscriber ->
				aSubscriber.onNext('Three')
				aSubscriber.onError(new Exception())
			}

		and:
			'A materialized stream is consumed'
			myStream.materialize().consume(
					{ println(it); res << it.type.toString() },                          // onNext
					{ it.printStackTrace() },                          // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['SUBSCRIBE', 'NEXT', 'ERROR']
	}

	def 'Streams can be dematerialized'() {
		when:
			'A source stream emits next signals followed by complete'
			def res = []
			def myStream = Streams.create { aSubscriber ->
				aSubscriber.onNext(Signal.next(1))
				aSubscriber.onNext(Signal.next(2))
				aSubscriber.onNext(Signal.next(3))
				aSubscriber.onNext(Signal.complete())
				aSubscriber.onNext(Signal.error(new Exception()))
			}

		and:
			'A dematerialized stream is consumed'
			myStream.dematerialize().consume(
					{ println(it); res << it },                          // onNext
					{ it.printStackTrace() },                          // onError
					{ println("Sequence complete"); res << 'complete' }          // onComplete
			)

		then:
			res == [1, 2, 3, 'complete']
	}


	def 'Streams can be switched'() {
		when:
			'A source stream emits next signals followed by an error'
			def res = []
			def myStream = Streams.create { aSubscriber ->
				aSubscriber.onNext('Three')
				aSubscriber.onNext('Two')
				aSubscriber.onNext('One')
			}

		and:
			'Another stream will emit values and complete'
			def myFallback = Streams.create { aSubscriber ->
				aSubscriber.onNext('0')
				aSubscriber.onNext('1')
				aSubscriber.onNext('2')
				aSubscriber.onComplete()
			}

		and:
			'The streams are switched'
			def switched = Streams.switchOnNext(Streams.just(myStream, myFallback))
			switched.consume(
					{ println(Thread.currentThread().name + ' ' + it); res << it },                          // onNext
					{ println("Error: " + it.message) }, // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == ['Three', 'Two', 'One', '0', '1', '2', 'complete']
	}

	def 'Streams can be switched dynamically'() {
		when:
			'A source stream emits next signals followed by an error'
			def res = []
			def myStream = Streams.<Integer> create { aSubscriber ->
				aSubscriber.onNext(1)
				aSubscriber.onNext(2)
				aSubscriber.onNext(3)
				aSubscriber.onComplete()
			}

		and:
			'The streams are switched'
			def switched = myStream.log('lol').switchMap { Streams.range(it, 3) }.log()
			switched.consume(
					{ println(Thread.currentThread().name + ' ' + it); res << it },                          // onNext
					{ println("Error: " + it.message) }, // onError
					{ println("Sequence complete"); res << 'complete' }          // onCompleted
			)

		then:
			res == [1, 2, 3, 2, 3, 3, 'complete']
	}


	def 'onOverflowDrop will miss events non requested'() {
		given:
			'a source and a timeout'
			def source = Streams.<Integer> broadcast()

			def value = null
			def tail = source.onOverflowDrop().observe { value = it }.log('overflow-drop-test').consume(5)
			println tail.debug()

		when:
			'the first values are accepted on the source, but we only have 5 requested elements out of 6'
			source.broadcastNext(1)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastNext(4)
			source.broadcastNext(5)

			println tail.debug()
			source.broadcastNext(6)

		and:
			'we try to consume the tail to check if 6 has been buffered'
			tail.requestMore(1)

		then:
			'last value known is 5'
			value == 5
	}

	def 'A Stream can be throttled'() {
		given:
			'a source and a throttled stream'
			def source = Streams.<Integer> broadcast(Environment.get())
			long avgTime = 150l

			def reduced = source
					.throttle(avgTime)
					.elapsed()
					.log()
					.take(10)
					.reduce { Tuple2<Tuple2<Long, Integer>, Long> acc ->
				acc.t2 ? ((acc.t1.t1 + acc.t2) / 2) : acc.t1.t1
			}

			def value = reduced.log().next()
			println value.debug()

		when:
			'the first values are accepted on the source'
			for (int i = 0; i < 10000; i++) {
				source.onNext(1)
			}
			sleep(1500)
			println(((long) (value.await())) + " milliseconds on average")

		then:
			'the average elapsed time between 2 signals is greater than throttled time'
			value.get() >= avgTime * 0.6

	}


	def 'A Stream can be throttled with a backoff policy as a stream'() {
		given:
			'a source and a throttled stream'
			def source = Streams.<Integer> broadcast(Environment.get())
			long avgTime = 150l

			def reduced = source
					.requestWhen {
				it
						.flatMap { v ->
					Streams.period(avgTime, TimeUnit.MILLISECONDS).map { 1l }.take(v)
				}
			}
			.elapsed()
					.take(10)
					.log('reduce')
					.reduce { Tuple2<Tuple2<Long, Integer>, Long> acc ->
				acc.t2 ? ((acc.t1.t1 + acc.t2) / 2) : acc.t1.t1
			}

			def value = reduced.log('promise').next()
			println value.debug()

		when:
			'the first values are accepted on the source'
			for (int i = 0; i < 10000; i++) {
				source.onNext(1)
			}
			sleep(1500)
			println value.debug()
			println(((long) (value.await())) + " milliseconds on average")

		then:
			'the average elapsed time between 2 signals is greater than throttled time'
			value.get() >= avgTime * 0.6

	}

	def 'time-slices of average'() {
		given:
			'a source and a throttled stream'
			def source = Streams.<Integer> broadcast(Environment.get())
			def latch = new CountDownLatch(1)
			long avgTime = 150l

			def reduced = source
					.buffer()
					.throttle(avgTime)
					.map { timeWindow -> timeWindow.size() }
					.finallyDo { latch.countDown() }

			def value = reduced.tap()
			println source.debug()

		when:
			'the first values are accepted on the source'
			for (int i = 0; i < 100000; i++) {
				source.broadcastNext(1)
			}
			source.broadcastComplete()
			latch.await(10, TimeUnit.SECONDS)
			println value.get()
			println source.debug()

		then:
			'the average elapsed time between 2 signals is greater than throttled time'
			value.get() > 1

	}


	def 'Collect will accumulate values from multiple threads'() {
		given:
			'a source and a collected stream'
			def sum = new AtomicInteger()
			int length = 1000
			int batchSize = 333
			int latchCount = length / batchSize
			def latch = new CountDownLatch(latchCount)
			def head = Streams.<Integer> broadcast(Environment.get())
			head.partition(3).consume {
				s ->
					s
							.dispatchOn(Environment.cachedDispatcher())
							.map { it }
							.buffer(batchSize)
							.consume { List<Integer> ints ->
						println ints.size()
						sum.addAndGet(ints.size())
						latch.countDown()
					}
			}
		when:
			'values are accepted into the head'
			(1..length).each { head.broadcastNext(it) }
			latch.await(4, TimeUnit.SECONDS)

		then:
			'results contains the expected values'
			println head.debug()
			sum.get() == length - 1
	}

	def 'An Observable can consume values from a Stream'() {
		given:
			'a Stream and a Observable consumer'
			def d = Streams.<Integer> broadcast()
			Stream composable = d
			Observable observable = Mock(Observable)
			composable.notify('key', observable)

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
			Stream stream = Streams.from([1, 2, 3])
			Observable observable = Mock(Observable)

		when:
			'a stream consumer is registerd'
			stream.notify('key', observable)

		then:
			'the observable is notified of the values'
			3 * observable.notify('key', _)
	}


	def 'A Stream can be materialized for recursive signal or introspection'() {
		given:
			'a composable with an initial value'
			def stream = Streams.just('test')

		when:
			'the stream is retrieved and a consumer is dynamically added'
			def value = null

			stream.nest().consume { theStream ->
				theStream.consume {
					value = it
				}
			}

		then:
			'it is available'
			value == 'test'
	}


	def 'A Stream can be transformed into consumers for reuse'() {
		given:
			'a composable and its signal comsumers'
			def stream = Streams.<String> broadcast()
			def completeConsumer = stream.toBroadcastCompleteConsumer()
			def nextConsumer = stream.toBroadcastNextConsumer()

		when:
			'the stream is retrieved and consumer are triggered'
			def promise = stream.toList()
			nextConsumer.accept('test1')
			nextConsumer.accept('test2')
			nextConsumer.accept('test3')
			completeConsumer.accept(null)

		then:
			'it is available'
			promise.get() == ['test1', 'test2', 'test3']
	}

	def 'A Stream can be transformed into error consumer for reuse'() {
		given:
			'a composable and its signal comsumers'
			def stream = Streams.<String> broadcast()
			def errorConsumer = stream.toBroadcastErrorConsumer()
			def nextConsumer = stream.toBroadcastNextConsumer()

		when:
			'the stream is retrieved and consumer are triggered'
			def promise = stream.toList()
			nextConsumer.accept('test1')
			nextConsumer.accept('test3')
			errorConsumer.accept(new Exception())
			promise.get()

		then:
			'toList promise is under error'
			thrown(Exception)
	}

	def 'A Stream can be controlled to update its properties reactively'() {
		given:
			'a composable and its signal comsumers'
			def stream = Streams.<String> broadcast()
			def controlStream = Streams.<Integer> broadcast()

			stream.control(controlStream) { it.t1.capacity(it.t2) }

		when:
			'the control stream receives a new capacity size'
			controlStream.broadcastNext(100)

		then:
			'Stream has been updated'
			stream.capacity == 100
	}


	def 'A Stream can re-subscribe its oldest parent on error signals'() {
		given:
			'a composable with an initial value'
			def stream = Streams.from(['test', 'test2', 'test3'])

		when:
			'the stream triggers an exception for the 2 first elements and is using retry(2) to ignore them'
			def i = 0
			stream = stream.observe {
				println it
				if (i++ < 2) {
					throw new RuntimeException()
				}
			}.retry(2).observe { println it }.count()

			def value = stream.tap()

			println value.debug()

		then:
			'3 values are passed since it is a cold stream resubscribed 2 times and finally managed to get the 3 values'
			value.get() == 3

		when:
			'the stream triggers an exception for the 2 first elements and is using retry() to ignore them'
			i = 0
			stream = Streams.broadcast()
			def tap = stream.observe {
				if (i++ < 2) {
					throw new RuntimeException()
				}
			}.retry().count().tap()

			println tap.debug()
			println tap.debug()
			stream.broadcastNext('test')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')
			stream.broadcastComplete()
			println tap.debug()
			def res = tap.get()

		then:
			'it is a hot stream and only 1 value (the most recent) is available'
			res == 1

		when:
			'the stream triggers an exception for the 2 first elements and is using retry(matcher) to ignore them'
			i = 0
			stream = Streams.from(['test', 'test2', 'test3'])
			value = stream.observe {
				if (i++ < 2) {
					throw new RuntimeException()
				}
			}.retry {
				i == 1
			}.count().tap()
			println value.debug()

		then:
			'3 values are passed since it is a cold stream resubscribed 1 time'
			value.get() == 3

		when:
			'the stream triggers an exception for the 2 first elements and is using retry(matcher) to ignore them'
			i = 0
			stream = Streams.broadcast()
			tap = stream.observe {
				if (i++ < 2) {
					throw new RuntimeException()
				}
			}.retry {
				i == 1
			}.count().tap()
			stream.broadcastNext('test')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')
			stream.broadcastComplete()
			println tap.debug()

		then:
			'it is a hot stream and only 1 value (the most recent) is available'
			tap.get() == 1
	}

	def 'A Stream can re-subscribe its oldest parent on error signals after backoff stream'() {
		when:
			'when composable with an initial value'
			def counter = 0
			def value = Streams.create {
				counter++
				it.onError(new RuntimeException("always fails"))
			}.retryWhen { attempts ->
				attempts.zipWith(Streams.range(1, 3)) { it.t2 }.flatMap { i ->
					println "delay retry by " + i + " second(s)"
					Streams.timer(i)
				}
			}.next()
			value.await()

		then:
			'Promise completed after 3 tries'
			value.isComplete()
			counter == 4
	}

	def 'A Stream can re-subscribe its oldest parent on complete signals'() {
		given:
			'a composable with an initial value'
			def stream = Streams.from(['test', 'test2', 'test3'])

		when:
			'using repeat(2) to ignore complete twice and resubscribe'
			def i = 0
			stream = stream.repeat(2).observe { i++ }.count()

			def value = stream.tap()

			println value.debug()

		then:
			'9 values are passed since it is a cold stream resubscribed 2 times and finally managed to get the 9 values'
			value.get() == 9


		when:
			'using repeat() to ignore complete and resubscribe'
			stream = Streams.broadcast()
			i = 0
			def tap = stream.repeat().observe { i++ }.consume()

			stream.broadcastNext('test')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')
			stream.broadcastComplete()

			stream.broadcastNext('test')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')
			stream.broadcastComplete()

			stream.broadcastNext('test')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')

		then:
			'it is a hot stream and only 9 value (the most recent) is available'
			i == 9
	}

	def 'A Stream can re-subscribe its oldest parent on complete signals after backoff stream'() {
		when:
			'when composable with an initial value'
			def counter = 0
			def value = Streams.create {
				counter++
				it.onComplete()
			}.repeatWhen { attempts ->
				attempts.zipWith(Streams.range(1, 3)) { it.t2 }.flatMap { i ->
					println "delay repeat by " + i + " second(s)"
					Streams.timer(i)
				}
			}.next()
			value.await()

		then:
			'Promise completed after 3 tries'
			value.isComplete()
			counter == 4
	}

	def 'A Stream can be timestamped'() {
		given:
			'a composable with an initial value and a relative time'
			def stream = Streams.just('test')
			def timestamp = System.currentTimeMillis()

		when:
			'timestamp operation is added and the stream is retrieved'
			def value = stream.timestamp().tap().get()

		then:
			'it is available'
			value.t1 >= timestamp
			value.t2 == 'test'
	}

	def 'A Stream can be benchmarked'() {
		given:
			'a composable with an initial value and a relative time'
			def stream = Streams.just('test')
			long timestamp = System.currentTimeMillis()

		when:
			'elapsed operation is added and the stream is retrieved'
			def value = stream.observe {
				sleep(1000)
			}.elapsed().tap().get()

			long totalElapsed = System.currentTimeMillis() - timestamp

		then:
			'it is available'
			value.t1 <= totalElapsed
			value.t2 == 'test'
	}

	def 'A Stream can be sorted'() {
		given:
			'a composable with an initial value and a relative time'
			def stream = Streams.from([43, 32122, 422, 321, 43, 443311])

		when:
			'sorted operation is added and the stream is retrieved'
			def value = stream.sort().buffer().tap()

		then:
			'it is available'
			value.get() == [43, 43, 321, 422, 32122, 443311]

		when:
			'a composable with an initial value and a relative time'
			stream = Streams.from([43, 32122, 422, 321, 43, 443311])

		and:
			'sorted operation is added for up to 3 elements ordered at once and the stream is retrieved'
			value = stream.sort(3).buffer(6).tap()
			println value.debug()

		then:
			'it is available'
			value.get() == [43, 422, 32122, 43, 321, 443311]

		when:
			'a composable with an initial value and a relative time'
			stream = Streams.from([1, 2, 3, 4])

		and:
			'revese sorted operation is added and the stream is retrieved'
			value = stream
					.sort({ a, b -> b <=> a } as Comparator<Integer>)
					.buffer()
					.tap()

		then:
			'it is available'
			value.get() == [4, 3, 2, 1]
	}

	def 'A Stream can be limited'() {
		given:
			'a composable with an initial values'
			def stream = Streams.from(['test', 'test2', 'test3'])

		when:
			'take to the first 2 elements'
			def value = stream.take(2).tap().get()

		then:
			'the second is the last available'
			value == 'test2'

		when:
			'take until test2 is seen'
			stream = Streams.broadcast()
			def value2 = stream.takeWhile {
				'test2' != it
			}.tap()

			stream.broadcastNext('test1')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')

		then:
			'the second is the last available'
			value2.get() == 'test2'
	}


	def 'A Stream can be limited in time'() {
		given:
			'a composable with an initial values'
			def stream = Streams.range(0, Integer.MAX_VALUE)
					.dispatchOn(Environment.cachedDispatcher())

		when:
			'take to the first 2 elements'
			Streams.await(stream.take(2, TimeUnit.SECONDS))

		then:
			'the second is the last available'
			notThrown(Exception)
	}

	static class SimplePojo {
		int id
		String title

		int hashcode() { id }
	}

	static class Entity {
		String key
		String payload
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
