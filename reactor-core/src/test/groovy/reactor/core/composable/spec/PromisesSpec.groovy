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
import reactor.core.composable.Deferred
import reactor.core.composable.Promise
import reactor.core.Observable
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.GroovyTestUtils.*
/**
 * @author Stephane Maldini
 * @author Andy Wilkinson
 * @author Jon Brisbin
 */
class PromisesSpec extends Specification {

  def "An onComplete consumer is called when a promise is rejected"() {
    given:
      "a Promise with an onComplete Consumer"
      def deferred = Promises.<Object> defer().get()
      def promise = deferred.compose()
      def acceptedPromise

      promise.onComplete(consumer { self ->
        acceptedPromise = self
      })

    when:
      "the promise is rejected"
      deferred.accept new Exception()

    then:
      "the consumer is invoked with the promise"
      acceptedPromise == promise
      promise.error
  }

  def "An onComplete consumer is called when added to an already-rejected promise"() {
    given:
      "a rejected Promise"
      def promise = Promises.<Object> error(new Exception()).get()

    when:
      "an onComplete consumer is added"
      def acceptedPromise

      promise.onComplete(consumer { self ->
        acceptedPromise = self
      })

    then:
      "the consumer is invoked with the promise"
      acceptedPromise == promise
      promise.error
  }

  def "An onComplete consumer is called when a promise is fulfilled"() {
    given:
      "a Promise with an onComplete Consumer"
      def deferred = Promises.<Object> defer().get()
      def promise = deferred.compose()
      def acceptedPromise

      promise.onComplete(consumer { self ->
        acceptedPromise = self
      })

    when:
      "the promise is fulfilled"
      deferred.accept 'test'

    then:
      "the consumer is invoked with the promise"
      acceptedPromise == promise
      promise.success
  }

  def "An onComplete consumer is called when added to an already-fulfilled promise"() {
    given:
      "a fulfilled Promise"
      def promise = Promises.success('test').get()

    when:
      "an onComplete consumer is added"
      def acceptedPromise

      promise.onComplete(consumer { self ->
        acceptedPromise = self
      })

    then:
      "the consumer is invoked with the promise"
      acceptedPromise == promise
      promise.success
  }

  def "An onSuccess consumer is called when a promise is fulfilled"() {
    given:
      "a Promise with an onSuccess Consumer"
      def deferred = Promises.<Object> defer().get()
      def promise = deferred.compose()
      def acceptedValue

      promise.onSuccess(consumer { v ->
        acceptedValue = v
      })

    when:
      "the promise is fulfilled"
      deferred.accept 'test'

    then:
      "the consumer is invoked with the fulfilling value"
      acceptedValue == 'test'
  }

  def "An onSuccess consumer is called when added to an already-fulfilled promise"() {
    given:
      "a fulfilled Promise"
      def promise = Promises.success('test').get()

    when:
      "an onSuccess consumer is added"
      def acceptedValue

      promise.onSuccess(consumer { v ->
        acceptedValue = v
      })

    then:
      "the consumer is invoked with the fulfilling value"
      acceptedValue == 'test'
  }

  def "An onSuccess consumer can be added to an already-rejected promise"() {
    given:
      "a rejected Promise"
      def promise = Promises.error(new Exception()).get()

    when:
      "an onSuccess consumer is added"
      promise.onSuccess(consumer {})

    then:
      "no exception is thrown"
      notThrown Exception
  }

  def "An onError consumer can be added to an already-fulfilled promise"() {
    given:
      "a fulfilled Promise"
      def promise = Promises.success('test').get()

    when:
      "an onError consumer is added"
      promise.onSuccess(consumer {})

    then:
      "no exception is thrown"
      notThrown Exception
  }

  def "An onError consumer is called when a promise is rejected"() {
    given:
      "a Promise with an onError Consumer"
      def deferred = Promises.<Object> defer().get()
      def promise = deferred.compose()
      def acceptedValue

      promise.onError(consumer { v ->
        acceptedValue = v
      })

    when:
      "the promise is rejected"
      def failure = new Exception()
      deferred.accept failure

    then:
      "the consumer is invoked with the rejecting value"
      acceptedValue == failure
  }

  def "An onError consumer is called when added to an already-rejected promise"() {
    given:
      "a rejected Promise"
      def failure = new Exception()
      def promise = Promises.error(failure).get()

    when:
      "an onError consumer is added"
      def acceptedValue

      promise.onError(consumer { v ->
        acceptedValue = v
      })

    then:
      "the consumer is invoked with the rejecting value"
      acceptedValue == failure
  }

  def "When getting a rejected promise's value the exception that the promise was rejected with is thrown"() {
    given:
      "a rejected Promise"
      def failure = new Exception()
      def promise = Promises.error(failure).get()

    when:
      "getting the promise's value"
      promise.get()

    then:
      "the error that the promise was rejected with is thrown"
      thrown(Exception)
  }

  def "A fulfilled promise's value is returned by get"() {
    given:
      "a fulfilled Promise"
      def promise = Promises.success('test').get()

    when:
      "getting the promise's value"
      def value = promise.get()

    then:
      "the value used to fulfil the promise is returned"
      value == 'test'
  }

  def "A promise can be fulfilled with null"() {
    given:
      "a promise"
      def deferred = Promises.<Object> defer().get()
      def promise = deferred.compose()

    when:
      "the promise is fulfilled with null"
      deferred.accept null

    then:
      "the promise was successful"
      promise.isComplete()
      promise.isSuccess()
  }

  def "An Observable can be used to consume a promise's value when it's fulfilled"() {
    given:
      "a promise with a consuming Observable"
      def deferred = Promises.<Object> defer().get()
      def promise = deferred.compose()
      def observable = Mock(Observable)
      promise.consume('key', observable)

    when:
      "the promise is fulfilled"
      deferred.accept 'test'

    then:
      "the observable is notified"
      1 * observable.notify('key', { event -> event.data == 'test' })
  }

  def "An Observable can be used to consume the value of an already-fulfilled promise"() {
    given:
      "a fulfilled promise"
      def promise = Promises.success('test').get()
      def observable = Mock(Observable)

    when:
      "an Observable is added as a consumer"
      promise.consume('key', observable)

    then:
      "the observable is notified"
      1 * observable.notify('key', { event -> event.data == 'test' })
  }

  def "A function can be used to map a Promise's value when it's fulfilled"() {
    given:
      "a promise with a mapping function"
      def deferred = Promises.<Integer> defer().get()
      def promise = deferred.compose()
      def mappedPromise = promise.map(function { it * 2 })

    when:
      "the original promise is fulfilled"
      deferred.accept 1

    then:
      "the mapped promise is fulfilled with the mapped value"
      mappedPromise.get() == 2
  }

  def "A function can be used to map an already-fulfilled Promise's value"() {
    given:
      "a fulfilled promise with a mapping function"
      def promise = Promises.success(1).get()

    when:
      "a mapping function is added"
      def mappedPromise = promise.map(function { it * 2 })

    then:
      "the mapped promise is fulfilled with the mapped value"
      mappedPromise.get() == 2
  }

  def "An onSuccess consumer registered via then is called when the promise is fulfilled"() {
    given:
      "A promise with an onSuccess consumer registered using then"
      Deferred<String, Promise<String>> deferred = Promises.<String> defer().get()
      Promise<String> promise = deferred.compose()
      def value = null
      promise.then(consumer { value = it }, null)

    when:
      "The promise is fulfilled"
      deferred.accept 'test'

    then:
      "the consumer is called"
      value == 'test'
  }

  def "An onError consumer registered via then is called when the promise is rejected"() {
    given:
      "A promise with an onError consumer registered using then"
      Deferred<String> promise = Promises.<String> defer().synchronousDispatcher().get()
      def value
	    promise.compose().then(consumer{}, consumer { value = it })

	  when:
      "The promise is rejected"
      def e = new Exception()
      promise.accept e

    then:
      "the consumer is called"
      value == e
  }

  def "An onError consumer registered via then is called when the promise is already rejected"() {
    given:
      "A promise that has been rejected"
      def e = new Exception()
      def promise = Promises.<String> error(e).synchronousDispatcher().get()

    when:
      "An onError consumer is registered via then"
      def value
      promise.then(consumer{}, consumer { value = it })

    then:
      "The consumer is called"
      value == e
  }

  def "An onSuccess consumer registered via then is called when the promise is already fulfilled"() {
    given:
      "A promise that has been fulfilled"
      def promise = Promises.success('test').synchronousDispatcher().get()

    when:
      "An onSuccess consumer is registered via then"
      def value
      promise.then(consumer { value = it }, null)

    then:
      "The consumer is called"
      value == 'test'
  }

  def "An onSuccess function registered via then is called when the promise is fulfilled"() {
    given:
      "a promise with an onSuccess function registered using then"
      Deferred<String> promise = Promises.<String> defer().synchronousDispatcher().get()
      def transformed = promise.compose().then(function { it * 2 }, null)

    when:
      "the promise is fulfilled"
      promise.accept 1

    then:
      "the function is called and the transformed promise is fulfilled"
      transformed.get() == 2
  }

  def "An onSuccess function registered via then is called when the promise is already fulfilled"() {
    given:
      "A promise that has been fulfilled"
      def promise = Promises.success(1).synchronousDispatcher().get()

    when:
      "An onSuccess function is registered via then"
      def transformed = promise.then(function { it * 2 }, null)

    then:
      "The function is called and the transformed promise is fulfilled"
	    transformed.success
	    transformed.get() == 2
  }

  def "When a promise is fulfilled, if a mapping function throws an exception the mapped promise is rejected"() {
    given:
      "a promise with a filter that throws an exception"
      Deferred<String> promise = Promises.<String> defer().synchronousDispatcher().get()
      def e = new RuntimeException()
      def mapped = promise.compose().map(function { throw e })

    when:
      "the promise is fulfilled"
      promise.accept 2

    then:
      "the mapped promise is rejected"
      mapped.error
  }

  def "When a promise is already fulfilled, if a mapping function throws an exception the mapped promise is rejected"() {
    given:
      "a fulfilled promise"
      def promise = Promises.success(1).synchronousDispatcher().get()

    when:
      "a mapping function that throws an exception is added"
      def e = new RuntimeException()
      def mapped = promise.map(function { throw e })

    then:
      "the mapped promise is rejected"
      mapped.error
  }

  def "An IllegalStateException is thrown if an attempt is made to fulfil a fulfilled promise"() {
    given:
      "a fulfilled promise"
      def promise = Promises.defer().synchronousDispatcher().get()

    when:
      "an attempt is made to fulfil it"
      promise.accept 1
      promise.accept 1

    then:
      "an IllegalStateException is thrown"
      thrown(IllegalStateException)
  }

	def "An IllegalStateException is thrown if an attempt is made to reject a rejected promise"() {
		given:
			"a rejected promise"
			Deferred promise = Promises.defer().synchronousDispatcher().get()

		when:
			"an attempt is made to fulfil it"
			promise.accept new Exception()
			promise.accept new Exception()

		then:
			"an IllegalStateException is thrown"
			thrown(IllegalStateException)
	}

  def "An IllegalStateException is thrown if an attempt is made to reject a fulfilled promise"() {
    given:
      "a fulfilled promise"
      def promise = Promises.defer().synchronousDispatcher().get()

    when:
      "an attempt is made to fulfil it"
      promise.accept 1
      promise.accept new Exception()

    then:
      "an IllegalStateException is thrown"
      thrown(IllegalStateException)
  }

  def "Multiple promises can be combined"() {
    given:
      "two fulfilled promises"
      Deferred promise1 = Promises.defer().synchronousDispatcher().get()
      Deferred promise2 = Promises.defer().synchronousDispatcher().get()

    when:
      "a combined promise is first created"
      Promise combined = Promises.when(promise1, promise2)

    then:
      "it is pending"
	    combined.pending

    when:
      "the first promise is fulfilled"
      promise1.accept 1

    then:
      "the combined promise is still pending"
      combined.pending

    when:
      "the second promise if fulfilled"
      promise2.accept 2

    then:
      "the combined promise is fulfilled with both values"
	    combined.success
	    combined.get() == [1, 2]
  }

  def "A combined promise is rejected once any of its component promises are rejected"() {
    given:
      "two unfulfilled promises"
      def promise1 = Promises.defer().synchronousDispatcher().get()
      def promise2 = Promises.defer().synchronousDispatcher().get()

    when:
      "a combined promise is first created"
      def combined = Promises.when(promise1, promise2)

    then:
      "it is pending"
	    combined.pending

    when:
      "a component promise is rejected"
      promise1.accept new Exception()

    then:
      "the combined promise is rejected"
	    combined.error
  }

  def "A combined promise is immediately fulfilled if its component promises are already fulfilled"() {
    given:
      "two fulfilled promises"
      def promise1 = Promises.success(1).get()
      def promise2 = Promises.success(2).get()

    when:
      "a combined promise is first created"
      def combined = Promises.when(promise1, promise2)

    then:
      "it is fulfilled"
	    combined.success
	    combined.get() == [1, 2]

	  when:
		  "promises are supplied"
		  promise1 = Promises.task(supplier{'1'}).get()
		  promise1.flush()
		  promise2 = Promises.task(supplier{'2'}).get()
		  promise2.flush()
	    combined = Promises.when(promise1, promise2)


	  then:
		  "it is fulfilled"
		  combined.success
		  combined.get() == ['1', '2']

  }

	def "A combined promise through 'any' is fulfilled with the first component result when using synchronously"() {
		given:
			"two fulfilled promises"
			def promise1 = Promises.success(1).get()
			def promise2 = Promises.success(2).get()

		when:
			"a combined promise is first created"
			def combined = Promises.any(promise1, promise2)

		then:
			"it is fulfilled"
			combined.success
			combined.get() == 1
	}

  def "A combined promise is immediately rejected if its component promises are already rejected"() {
    given:
      "two rejected promises"
      def promise1 = Promises.error(new Exception()).synchronousDispatcher().get()
      def promise2 = Promises.error(new Exception()).synchronousDispatcher().get()

    when:
      "a combined promise is first created"
      def combined = Promises.when(promise1, promise2)

    then:
      "it is rejected"
      combined.error
  }

  def "A single promise can be 'combined'"() {
    given:
      "one unfulfilled promise"
      def promise1 = Promises.defer().synchronousDispatcher().get()

    when:
      "a combined promise is first created"
      def combined = Promises.when(promise1)

    then:
      "it is pending"
      combined.pending

    when:
      "the first promise is fulfilled"
      promise1.accept 1

    then:
      "the combined promise is fulfilled"
	    combined.await() == [1]
	    combined.success
  }

  def "A promise can be fulfilled with a Supplier"() {
    when:
      "A promise configured with a supplier"
      def promise = Promises.task(supplier { 1 }).get()

    then:
      "it is fulfilled"
      promise.get() == 1
	    promise.success
  }

  def "A promise with a Supplier that throws an exception is rejected"() {
    when:
      "A promise configured with a supplier that throws an exception"
      def promise = Promises.task(supplier { throw new RuntimeException() }).get()
	    promise.get()

	  then:
      "it is rejected"
	    thrown RuntimeException
      promise.error
  }

  def "A filtered promise is not fulfilled if the filter does not allow the value to pass through"() {
    given:
      "a promise with a filter that only accepts even values"
      def promise = Promises.defer().synchronousDispatcher().get()
      def filteredPromise = promise.compose().filter(predicate { it % 2 == 0 })

    when:
      "the promise is fulfilled with an odd value"
      promise.accept 1

    then:
      "the filtered promise is not fulfilled"
      filteredPromise.pending
  }

  def "A filtered promise is fulfilled if the filter allows the value to pass through"() {
    given:
      "a promise with a filter that only accepts even values"
      def promise = Promises.defer().synchronousDispatcher().get()
      def filteredPromise = promise.compose().filter(predicate { it % 2 == 0 })

    when:
      "the promise is fulfilled with an even value"
      promise.accept 2

    then:
      "the filtered promise is fulfilled"
      filteredPromise.success
      filteredPromise.get() == 2
  }

  def "If a filter throws an exception the filtered promise is rejected"() {
    given:
      "a promise with a filter that throws an exception"
      def promise = Promises.defer().synchronousDispatcher().get()
      def e = new RuntimeException()
      def filteredPromise = promise.compose().filter(predicate { throw e })

    when:
      "the promise is fulfilled"
      promise.accept 2

    then:
      "the filtered promise is rejected"
      filteredPromise.error
  }

  def "If a promise is already fulfilled with a value accepted by a filter the filtered promise is fulfilled"() {
    given:
      "a promise that is already fulfilled with an even value"
      def promise = Promises.success(2).synchronousDispatcher().get()

    when:
      "the promise is filtered with a filter that only accepts even values"
      def filteredPromise = promise.filter(predicate { it % 2 == 0 })

    then:
      "the filtered promise is fulfilled"
      filteredPromise.success
      filteredPromise.get() == 2
  }

  def "If a promise is already fulfilled with a value rejected by a filter, the filtered promise is not fulfilled"() {
    given:
      "a promise that is already fulfilled with an odd value"
      def promise = Promises.success(1).synchronousDispatcher().get()

    when:
      "the promise is filtered with a filter that only accepts even values"
      def filteredPromise = promise.filter(predicate { it % 2 == 0 })

    then:
      "the filtered promise is not fulfilled"
      filteredPromise.pending
  }

  def "Errors stop compositions"() {
    given:
      "a promise"
      def promiseDeferred = Promises.<String> defer()
		      .env(new Environment())
		      .dispatcher('eventLoop')
		      .get()

      final latch = new CountDownLatch(1)

    when:
      "p1 is consumed by p2"
      Promise s = promiseDeferred.compose().then(function { Integer.parseInt it }, null).
          when(NumberFormatException, consumer { latch.countDown() }).
          then(function { println('not in log'); true }, null)

    and:
      "setting a value"
      promiseDeferred.accept 'not a number'
      s.await(5000, TimeUnit.MILLISECONDS)

    then:
      'No value'
	    thrown(RuntimeException)
	    latch.count == 0
  }

}

