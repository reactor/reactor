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

import com.fasterxml.jackson.databind.ObjectMapper
import reactor.core.Environment
import reactor.core.Observable
import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.event.dispatch.SynchronousDispatcher
import reactor.event.selector.Selectors
import reactor.function.Function
import reactor.function.support.Tap
import reactor.rx.Stream
import reactor.tuple.Tuple2
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class StreamsSpec extends Specification {

	@Shared
	Environment environment

	void setup() {
		environment = new Environment()
	}

	def cleanup() {
		environment.shutdown()
	}

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

	def 'A deferred Stream can listen for terminal states'() {
		given:
			'a composable with an initial value'
			Stream stream = Streams.defer('test')

		when:
			'the complete signal is observed and stream is retrieved'
			def value = null

			stream.finallyDo {
				value = it
			}.tap()

			println stream.debug()

		then:
			'it is available'
			value == stream
			value.state == Stream.State.COMPLETE
	}

	def 'A deferred Stream can be run on various dispatchers'() {
		given:
			'a composable with an initial value'
			def dispatcher1 = new SynchronousDispatcher()
			def dispatcher2 = new SynchronousDispatcher()
			def stream = Streams.defer(null, dispatcher1, 'test', 'test2', 'test3')

		when:
			'the stream is retrieved'
			def tail = stream.dispatchOn(dispatcher2)

		then:
			'it is available'
			stream.dispatcher == dispatcher1
			tail.dispatcher == dispatcher2
	}

	def 'A deferred Stream can be translated into a list'() {
		given:
			'a composable with an initial value'
			Stream stream = Streams.defer('test', 'test2', 'test3')

		when:
			'the stream is retrieved'
			def value = stream.map { it + '-ok' }.toList()

			println stream.debug()

		then:
			'it is available'
			value.get() == ['test-ok', 'test2-ok', 'test3-ok']
	}

	def 'A deferred Stream can be translated into a completable queue'() {
		given:
			'a composable with an initial value'
			def stream = Streams.defer(environment, 'test', 'test2', 'test3')

		when:
			'the stream is retrieved'
			def queue = stream.map { it + '-ok' }.toBlockingQueue()

			def res
			def result = []

			for (; ;) {
				res = queue.poll()

				if (res)
					result << res

				if (queue.isComplete())
					break
			}
			println stream.debug()

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
			def tap = s.distinctUntilChanged().buffer().tap()

		then:
			'collected must remove duplicates'
			tap.get() == [1, 2, 3]
	}

	def "A Stream's initial values are passed to consumers"() {
		given:
			'a composable with values 1 to 5 inclusive'
			Stream stream = Streams.defer([1, 2, 3, 4, 5])

		when:
			'a Consumer is registered'
			def values = []
			stream.consume { values << it }

		then:
			'the initial values are passed'
			values == [1, 2, 3, 4, 5]


	}

	def 'Accepted values are passed to a registered Consumer'() {
		given:
			'a composable with a registered consumer'
			def composable = Streams.<Integer> defer()
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
			Stream composable = Streams.<Integer> defer()
			def errors = 0
			composable.when(RuntimeException) { errors++ }
			println composable.debug()

		when:
			'A RuntimeException is accepted'
			composable.broadcastError(new RuntimeException())

		then:
			'it is passed to the consumer'
			errors == 1

		when:
			'A new error consumer is subscribed'
			composable.when(RuntimeException) { errors++ }

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
			def d = Streams.<Integer> config().capacity(3).get()
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
			tap = composable.every(3).tap()
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
			def source = Streams.<Integer> defer()
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
			def source = Streams.<Integer> defer()
			Stream<Integer> mapped = source.
					flatMap { Integer v -> println v; Streams.<Integer> defer(v * 2) }

		when:
			'the source accepts a value'
			def value = mapped.tap()
			println source.debug()
			source.broadcastNext(1)
			println source.debug()

		then:
			'the value is mapped'
			value.get() == 2
	}

	def "Multiple Stream's values can be merged"() {
		given:
			'source composables to merge, buffer and tap'
			def source1 = Streams.<Integer> defer()
			def source2 = Streams.<Integer> defer().map { it }.map { it }
			def source3 = Streams.<Integer> defer()
			def tap = source1.merge(source2, source3).buffer(3).tap()

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
			def source2 = Streams.<Integer> defer()
			def source3 = Streams.<Integer> defer()
			def source1 = Streams.<Stream<Integer>> defer(source2, source3)
			def tap = source1.join().tap()

		when:
			'the sources accept a value'
			source2.broadcastNext(1)
			source3.broadcastNext(2)
			source3.broadcastNext(3)
			source3.broadcastNext(4)

			println source1.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == [1, 2]

		when:
			'the sources accept the missing value'
			source3.broadcastNext(5)
			source2.broadcastNext(6)

			println source1.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == [3, 6]
	}

	def "Multiple Stream's values can be zipped"() {
		given:
			'source composables to merge, buffer and tap'
			def source2 = Streams.<Integer> defer()
			def source3 = Streams.<Integer> defer()
			def source1 = Streams.<Stream<Integer>> defer(source2, source3)
			def tap = source1.zip { it.sum() }.tap()

		when:
			'the sources accept a value'
			source2.broadcastNext(1)
			source3.broadcastNext(2)
			source3.broadcastNext(3)
			source3.broadcastNext(4)

			println source1.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == 3

		when:
			'the sources accept the missing value'
			source3.broadcastNext(5)
			source2.broadcastNext(6)

			println source1.debug()

		then:
			'the values are all collected from source1 stream'
			tap.get() == 9
	}


	def "Stream can be counted"() {
		given:
			'source composables to count and tap'
			def source = Streams.<Integer> defer()
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
			def source = Streams.<Integer> defer()
			Stream filtered = source.filter { it % 2 == 0 }

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
			def rejectedSource = filtered.filter { false }.otherwise()
			def rejectedTap = rejectedSource.tap()
			source.broadcastNext(2)
			println source.debug()

		then:
			'it is rejected by the filter'
			rejectedTap.get() == 2

		when:
			'simple filter'
			def anotherSource = Streams.<Boolean> defer()
			def tap = anotherSource.filter().tap()
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
			def source = Streams.<Integer> defer()
			Stream mapped = source.map { if (it == 1) throw new RuntimeException() else 'na' }
			def errors = 0
			mapped.when(Exception) { errors++ }
			mapped.available()

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
			def source = Streams.<Integer> defer()
			Stream filtered = source.filter { if (it == 1) throw new RuntimeException() else true }
			def errors = 0
			filtered.when(Exception) { errors++ }
			filtered.available()

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
			Stream reduced = Streams.<Integer> config().each([1, 2, 3, 4, 5]).
					synchronousDispatcher().
					get().
					reduce(new Reduction())

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
			def source = Streams.<Integer> config().capacity(5).get()
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

		then:
			'the consumer only receives the final value'
			values == [120]
	}

	def 'A known number of values can be reduced'() {
		given:
			'a composable that will accept 5 values and a reduce function'
			def source = Streams.<Integer> config().capacity(5).get()
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
			def source = Streams.<Integer> config().capacity(2).get()
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
			reduced.available()

		then:
			'the updated reduction is available'
			value.get() == 2

		when:
			'use an initial value'
			reduced = source.reduce(new Reduction(), 2)
			value = reduced.tap()
			source.broadcastNext(1)
			reduced.available()

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
			def source = Streams.<Integer> config().capacity(1).get()
			Stream reduced = source.buffer()
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
			def source = Streams.<Integer> config().capacity(2).get()
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

	def 'Window will re-route N elements to a fresh nested stream'() {
		given:
			'a source and a collected window stream'
			def source = Streams.<Integer> config().capacity(2).get()
			Tap<List<Integer>> value = null

			source.window().consume {
				value = it.buffer(2).tap()
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
			println source.debug()

		then:
			'the collected list contains the first and second elements'
			value.get() == [1, 2]

		when:
			'2 more values are accepted'
			source.broadcastNext(3)
			source.broadcastNext(4)
			println source.debug()

		then:
			'the collected list contains the first and second elements'
			value.get() == [3, 4]
	}

	def 'GroupBy will re-route N elements to a nested stream based on the mapped key'() {
		given:
			'a source and a grouped by ID stream'
			def source = Streams.<SimplePojo> defer()
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

	def 'StreamUtils will parse a Stream to a Map'() {
		given:
			'a source and a grouped by ID stream'
			def source = Streams.<SimplePojo> defer()

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
			source.broadcastComplete()
			def result = source.debug().toMap()

		then:
			'the result should contain all stream titles by id'
			result.to[0].id == "GroupBy"
			result.to[0].to[0].id == "Callback"
			result.to[0].boundTo[0].id == "1"
			result.to[0].boundTo[1].id == "2"
			result.to[0].boundTo[2].id == "3"
			result.to[0].boundTo[0].to[0].id == "Callback"
			result.to[0].boundTo[1].to[0].id == "Callback"
			result.to[0].boundTo[2].to[0].id == "Callback"
	}

	def 'Collect will accumulate a list of accepted values until flush and pass it to a consumer'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer> defer()
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
			reduced.available()

		then:
			'the collected list contains the first and second elements'
			value.get() == [1, 2]
	}

	def 'Creating Stream from environment'() {
		given:
			'a source stream with a given environment'
			def source = Streams.<Integer> defer(environment, environment.getDispatcher('ringBuffer'))
			def source2 = Streams.<Integer> defer(environment)

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


		cleanup:
			environment.shutdown()
	}

	def 'Creating Stream from observable'() {
		given:
			'a source stream with a given observable'
			def r = Reactors.reactor().get()
			def selector = Selectors.anonymous()
			int event = 0
			Streams.<Integer> on(r, selector).consume { event = it }

		when:
			'accept a value'
			r.notify(selector.object, Event.wrap(1))

		then:
			'dispatching works'
			event == 1
	}

	def 'Throttle will accumulate a list of accepted values and pass it to a consumer on the specified period'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer> config().env(environment).get()
			def reduced = source.buffer().throttle(300)
			def value = reduced.tap()

		when:
			'the first values are accepted on the source'
			source.broadcastNext(1)
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

		cleanup:
			environment.shutdown()

	}

	def 'Throttle will generate demand every specified period'() {
		given:
			'a source and a collected stream'
			def random = new Random()
			def source = Streams.generate(environment) {
				random.nextInt()
			}

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
			def source = Streams.<Integer> config().synchronousDispatcher().env(environment).get()
			Stream reduced = source.buffer(5).timeout(600)
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
			sleep(2000)
			println reduced.debug()

		then:
			'the collected list contains the first and second elements'
			value.get() == [2, 2]

		cleanup:
			environment.shutdown()

	}

	def 'A Stream can be throttled'() {
		given:
			'a source and a throttled stream'
			def source = Streams.<Integer> config().synchronousDispatcher().env(environment).get()
			long avgTime = 150l
			long nanotime = avgTime * 1_000_000

			def reduced = source
					.throttle(avgTime)
					.elapsed()
					.reduce { Tuple2<Tuple2<Long, Integer>, Long> acc ->
				acc.t2 ? ((acc.t1.t1 + acc.t2) / 2) : acc.t1.t1
			}

			def value = reduced.tap()
			println source.debug()

		when:
			'the first values are accepted on the source'
			for (int i = 0; i < 1000000; i++) {
				source.broadcastNext(1)
			}
			source.broadcastComplete()
			sleep(1500)
			println source.debug()
			println(((long) (value.get() / 1_000_000)) + " milliseconds on average")

		then:
			'the average elapsed time between 2 signals is greater than throttled time'
			value.get() >= nanotime * 0.6

		cleanup:
			environment.shutdown()

	}

	def 'Moving Buffer accumulate items without dropping previous'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer> config().synchronousDispatcher().env(environment).get()
			def reduced = source.movingBuffer(5).throttle(400)
			def value = reduced.tap()

		when:
			'the window accepts first items'
			source.broadcastNext(1)
			source.broadcastNext(2)
			sleep(1200)
			println source.debug()

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

	def 'Moving Buffer will drop overflown items'() {
		given:
			'a source and a collected stream'
			def source = Streams.<Integer> config().synchronousDispatcher().env(environment).get()
			def reduced = source.movingBuffer(5)

		when:
			'the buffer overflows'
			source.broadcastNext(1)
			source.broadcastNext(2)
			source.broadcastNext(3)
			source.broadcastNext(4)
			source.broadcastNext(5)
			source.broadcastNext(6)
			def value = reduced.tap()

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
			int length = 1000
			int batchSize = 333
			int latchCount = length / batchSize
			def latch = new CountDownLatch(latchCount)
			def head = Streams.<Integer> defer(environment)
			head.parallel().map {
				s -> s.map { it }
			}.merge()
					.buffer(batchSize)
					.consume { List<Integer> ints ->
				println ints.size()
				sum.addAndGet(ints.size())
				latch.countDown()
			}
		when:
			'values are accepted into the head'
			(1..length).each { head.broadcastNext(it) }
			latch.await(4, TimeUnit.SECONDS)

		then:
			'results contains the expected values'
			println head.debug()
			sum.get() == 999
	}

	def 'Collect will accumulate values from multiple threads in MP MC scenario'() {
		given:
			'a source and a collected stream'
			def sum = new AtomicInteger()
			int length = 1000
			int latchCount = length
			def latch = new CountDownLatch(latchCount)
			def head = Streams.<Integer> defer(environment)
			def parallels = Streams.<Integer> parallel(environment)

			head.parallel().consume {
				s -> s.map { it } .consume{ parallels.onNext(it) }
			}

			parallels.consume { s ->
				s.consume { int i ->
					sum.addAndGet(1)
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
			println parallels.debug()
			sum.get() == 1000
	}

	def 'An Observable can consume values from a Stream'() {
		given:
			'a Stream and a Observable consumer'
			def d = Streams.<Integer> defer()
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
			Stream stream = Streams.defer([1, 2, 3])
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
			def stream = Streams.defer('test')

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
			def stream = Streams.<String> defer()
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
			def stream = Streams.<String> defer()
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
			def stream = Streams.<String> defer()
			def controlStream = Streams.<Integer> defer()

			stream.control(controlStream) { it.t1.capacity(it.t2) }

		when:
			'the control stream receives a new capacity size'
			controlStream.broadcastNext(100)

		then:
			'Stream has been updated'
			stream.maxCapacity == 100
	}


	def 'A Stream can re-subscribe its oldest parent on error signals'() {
		given:
			'a composable with an initial value'
			def stream = Streams.defer(['test', 'test2', 'test3']).capacity(3)

		when:
			'the stream triggers an exception for the 2 first elements and is using retry(2) to ignore them'
			def i = 0
			def value = stream.observe {
				if (i++ < 2) {
					throw new RuntimeException()
				}
			}.retry(2).count().tap().get()
			println stream.debug()

		then:
			'1 + 3 values are passed since it is a cold stream resubscribed 2 times'
			value == 4

		when:
			'the stream triggers an exception for the 2 first elements and is using retry() to ignore them'
			i = 0
			stream = Streams.defer()
			def value2 = stream.observe {
				if (i++ < 2) {
					throw new RuntimeException()
				}
			}.retry().count()
			stream.broadcastNext('test')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')
			stream.broadcastComplete()
			println stream.debug()

		then:
			'it is a hot stream and only 1 value (the most recent) is available'
			value2.tap().get() == 1

		when:
			'the stream triggers an exception for the 2 first elements and is using retry(matcher) to ignore them'
			i = 0
			stream = Streams.defer(['test', 'test2', 'test3']).capacity(3)
			value = stream.observe {
				if (i++ < 2) {
					throw new RuntimeException()
				}
			}.retry {
				i == 1
			}.count().tap().get()
			println stream.debug()

		then:
			'1 + 3 values are passed since it is a cold stream resubscribed 2 times'
			value == 4

		when:
			'the stream triggers an exception for the 2 first elements and is using retry(matcher) to ignore them'
			i = 0
			stream = Streams.defer()
			value2 = stream.observe {
				if (i++ < 2) {
					throw new RuntimeException()
				}
			}.retry {
				i == 1
			}.count()
			stream.broadcastNext('test')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')
			stream.broadcastComplete()
			println stream.debug()

		then:
			'it is a hot stream and only 1 value (the most recent) is available'
			value2.tap().get() == 1
	}


	def 'A Stream can be timestamped'() {
		given:
			'a composable with an initial value and a relative time'
			def stream = Streams.defer('test')
			def timestamp = System.nanoTime()

		when:
			'timestamp operation is added and the stream is retrieved'
			def value = stream.timestamp().tap().get()

		then:
			'it is available'
			value.t1 > timestamp
			value.t2 == 'test'
	}

	def 'A Stream can be benchmarked'() {
		given:
			'a composable with an initial value and a relative time'
			def stream = Streams.defer('test')
			long timestamp = System.nanoTime()

		when:
			'elapsed operation is added and the stream is retrieved'
			def value = stream.observe {
				sleep(1000)
			}.elapsed().tap().get()

			long totalElapsed = System.nanoTime() - timestamp

		then:
			'it is available'
			value.t1 < totalElapsed
			value.t2 == 'test'
	}

	def 'A Stream can be sorted'() {
		given:
			'a composable with an initial value and a relative time'
			def stream = Streams.defer([43, 32122, 422, 321, 43, 443311])

		when:
			'sorted operation is added and the stream is retrieved'
			def value = stream.sort().buffer().tap().get()

		then:
			'it is available'
			value == [43, 43, 321, 422, 32122, 443311]

		when:
			'a composable with an initial value and a relative time'
			stream = Streams.defer([43, 32122, 422, 321, 43, 443311])

		and:
			'sorted operation is added for up to 3 elements ordered at once and the stream is retrieved'
			value = stream.sort(3).buffer(6).tap().get()

		then:
			'it is available'
			value == [43, 422, 32122, 43, 321, 443311]

		when:
			'a composable with an initial value and a relative time'
			stream = Streams.defer([1, 2, 3, 4])

		and:
			'revese sorted operation is added and the stream is retrieved'
			value = stream
					.sort({ a, b -> b <=> a } as Comparator<Integer>)
					.buffer()
					.tap()
					.get()

		then:
			'it is available'
			value == [4, 3, 2, 1]
	}

	def 'A Stream can be limited'() {
		given:
			'a composable with an initial values'
			def stream = Streams.defer(['test', 'test2', 'test3'])

		when:
			'limit to the first 2 elements'
			def value = stream.limit(2).tap().get()

		then:
			'the second is the last available'
			value == 'test2'

		when:
			'limit until test2 is seen'
			stream = Streams.defer()
			def value2 = stream.limit {
				'test2' == it
			}.tap()

			stream.broadcastNext('test1')
			stream.broadcastNext('test2')
			stream.broadcastNext('test3')

		then:
			'the second is the last available'
			value2.get() == 'test2'
	}

	static class SimplePojo {
		int id
		String title
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
