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
import reactor.function.Function
import reactor.core.Observable
import reactor.tuple.Tuple2
import spock.lang.Ignore
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import static reactor.GroovyTestUtils.*

class StreamsSpec extends Specification {

  def 'A deferred Stream with an initial value makes that value available immediately'() {
    given:
      'a composable with an initial value'
      Deferred d = Streams.defer('test').synchronousDispatcher().get()

    when:
      'the value is retrieved'
      def value = d.compose().tap()
      d.compose().flush()

    then:
      'it is available'
      value.get() == 'test'
  }

  def 'A deferred Stream with an initial value passes the value to a consumer'() {
    given:
      'a composable with an initial value'
      def values = []
      Deferred d = Streams.defer(1).synchronousDispatcher().get()
      d.compose().consume(consumer { values << it }).flush()

    when:
      'a value is accepted'
      d.accept 2

    then:
      'the consumer has been passed the init value'
      values == [1, 2]
  }

  def 'A Stream with a known set of values makes those values available immediately'() {
    given:
      'a composable with values 1 to 5 inclusive'
      Stream s = Streams.defer([1, 2, 3, 4, 5]).synchronousDispatcher().get().compose()

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
      last.get() == 5
  }

  def "A Stream's initial values are not passed to consumers but subsequent values are"() {
    given:
      'a composable with values 1 to 5 inclusive'
      Deferred d = Streams.defer([1, 2, 3, 4, 5]).synchronousDispatcher().get()
      Composable composable = d.compose()

    when:
      'a Consumer is registered'
      def values = []
      composable.consume(consumer { values << it })

    then:
      'it is not called with the initial values'
      values == []

    when:
      'flush is called'
      composable.flush()

    then:
      'the initial values are passed'
      values == [1, 2, 3, 4, 5]

    when:
      'a subsequent value is accepted'
      d.accept(6)

    then:
      'it passed to the consumer'
      values == [1, 2, 3, 4, 5, 6]


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
      'the child contains the error from the parent'
      error instanceof Exception
  }

  def 'When the expected accept count is exceeded, last is updated with each new value'() {
    given:
      'a composable with a known number of values'
      Deferred d = Streams.defer([1, 2, 3, 4, 5]).synchronousDispatcher().get()
      Stream composable = d.compose()

    when:
      'last is retrieved'
      def last = composable.last().tap()
      composable.flush()

    then:
      'its value is the last of the initial values'
      last.get() == 5

    when:
      'another value is accepted'
      d.accept(6)

    then:
      'the value of last is not updated'
      last.get() == 5
  }

  def 'When the number of values is unknown, last is never updated'() {
    given:
      'a composable that will accept an unknown number of values'
      Deferred d = Streams.defer().synchronousDispatcher().get()
      Stream composable = d.compose()

    when:
      'last is retrieved'
      def last = composable.last().tap()

    then:
      'its value is unknown'
      last.get() == null

    when:
      'a value is accepted'
      d.accept(1)

    then:
      "last's value is still unknown"
      last.get() == null
  }

  def 'Last value of a batch is accessible'() {
    given:
      'a composable that will accept an unknown number of values'
      Deferred d = Streams.defer().get()
      Stream composable = d.compose()

    when:
      'last is retrieved'
      Stream last = composable.last()

    then:
      'its value is unknown'
      last.tap().get() == null

    when:
      'the expected accept count is set and that number of values is accepted'
      def batched = composable.batch(3)
      last = batched.last()
      def tap = last.tap()
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

  def "A Stream's values can be filtered"() {
    given:
      'a source composable with a filter that rejects odd values'
      Deferred source = Streams.defer().get()
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
  }

  def "When a mapping function throws an exception, the mapped composable accepts the error"() {
    given:
      'a source composable with a mapping function that throws an exception'
      Deferred source = Streams.defer().get()
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
      Deferred source = Streams.defer([1, 2, 3, 4, 5]).synchronousDispatcher().get()

    when:
      'a reduce function is registered'
      def reduced = source.compose().reduce(new Reduction())
      def value = reduced.tap()
      reduced.flush()

    then:
      'the resulting composable holds the reduced value'
      value.get() == 120
  }

  def "When reducing a known set of values, only the final value is passed to consumers"() {
    given:
      'a composable with a known set of values and a reduce function'
      Stream reduced = Streams.defer([1, 2, 3, 4, 5]).
          synchronousDispatcher().
          get().
          compose().
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
      Deferred source = Streams.defer().batchSize(5).synchronousDispatcher().get()
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
      Deferred source = Streams.defer().batchSize(2).synchronousDispatcher().get()
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

  def 'When an unknown number of values is being reduced, each reduction is made available'() {
    given:
      'a composable with a reduce function'
      Deferred source = Streams.defer().synchronousDispatcher().get()
      def value = source.compose().reduce(new Reduction()).tap()

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
  }

  def 'When an unknown number of values is being reduced, each reduction is passed to a consumer'() {
    given:
      'a composable with a reduce function'
      Deferred source = Streams.defer().synchronousDispatcher().get()
      Stream reduced = source.compose().reduce(new Reduction())
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
      Deferred d = Streams.defer([1, 2, 3]).synchronousDispatcher().get()
      Stream composable = d.compose()
      Observable observable = Mock(Observable)

    when:
      'a composable consumer is registerd'
      composable.consume('key', observable)

      composable.flush() //  TODO This will pass if get is called, but I don't think it should be necessary

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
