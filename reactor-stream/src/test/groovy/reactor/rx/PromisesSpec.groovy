/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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

import reactor.Mono
import reactor.Processors
import reactor.core.error.CancelException
import reactor.core.error.ReactorFatalException
import reactor.rx.broadcast.Broadcaster
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Stephane Maldini
 * @author Andy Wilkinson
 * @author Jon Brisbin
 */
class PromisesSpec extends Specification {

  def "An onComplete consumer is called when a promise is rejected"() {
	given: "a Promise with an onComplete Consumer"
	def promise = Promise.ready()
	def acceptedPromise

	promise.doOnTerminate { success, failure -> acceptedPromise = failure }.to(Promise.ready())

	when: "the promise is rejected"
	promise.onError new Exception()

	then: "the consumer is invoked with the promise"
	acceptedPromise == promise.reason()
  }

  def "An onComplete consumer is called when added to an already-rejected promise"() {
	given: "a rejected Promise"
	def promise = Promise.<Object> error(new Exception())

	when: "an onComplete consumer is added"
	def acceptedPromise

	promise.doOnTerminate { data, failure -> acceptedPromise = failure }.to(Promise.ready())

	then: "the consumer is invoked with the promise"
	acceptedPromise == promise.reason()
  }

  def "An onComplete consumer is called when a promise is fulfilled"() {
	given: "a Promise with an onComplete Consumer"
	def promise = Promise.ready()
	def acceptedPromise

	promise.doOnTerminate() { v, error -> acceptedPromise = v }.to(Promise.prepare())

	when: "the promise is fulfilled"
	promise.onNext 'test'

	then: "the consumer is invoked with the promise"
	acceptedPromise == promise.get()
	promise.success
  }

  def "An onComplete consumer is called when added to an already-fulfilled promise"() {
	given: "a fulfilled Promise"
	def promise = Promise.success('test')

	when: "an onComplete consumer is added"
	def acceptedPromise

	promise.doOnTerminate{ self, err -> acceptedPromise = self}.to(Promise.prepare())

	then: "the consumer is invoked with the promise"
	acceptedPromise == promise.get()
	promise.success
  }

  def "An onSuccess consumer is called when a promise is fulfilled"() {
	given: "a Promise with an doOnSuccess Consumer"
	def promise = Promise.ready()
	def acceptedValue

	promise.doOnSuccess { v -> acceptedValue = v }.to(Promise.prepare())

	when: "the promise is fulfilled"
	promise.onNext 'test'

	then: "the consumer is invoked with the fulfilling value"
	acceptedValue == 'test'
  }

  def "An onSuccess consumer is called when added to an already-fulfilled promise"() {
	given: "a fulfilled Promise"
	def promise = Promise.success('test')

	when: "an doOnSuccess consumer is added"
	def acceptedValue

	promise.doOnSuccess { v -> acceptedValue = v }.to(Promise.prepare())

	then: "the consumer is invoked with the fulfilling value"
	acceptedValue == 'test'
  }

  def "An onSuccess consumer can be added to an already-rejected promise"() {
	given: "a rejected Promise"
	def promise = Promise.error(new Exception())

	when: "an doOnSuccess consumer is added"
	def ex = null
	promise.doOnError { ex = it }.to(Promise.prepare())

	then: "no error is thrown"
	ex in Exception
	notThrown Exception
  }

  def "An onError consumer can be added to an already-fulfilled promise"() {
	given: "a fulfilled Promise"
	def promise = Promise.success('test')

	when: "an doOnError consumer is added"
	promise.doOnSuccess {}

	then: "no error is thrown"
	notThrown Exception
  }

  def "An onError consumer is called when a promise is rejected"() {
	given: "a Promise with an doOnError Consumer"
	def promise = Promise.ready()
	def acceptedValue

	promise.doOnError { v -> acceptedValue = v }.to(Promise.prepare())

	when: "the promise is rejected"
	def failure = new Exception()
	promise.onError failure

	then: "the consumer is invoked with the rejecting value"
	acceptedValue == failure
  }

  def "A promise can only listen to terminal states"() {
	given: "a Promise with an doOnError Consumer"
	def promise = Promise.ready()
	def after = Promise.ready()
	promise.after().subscribe(after)

	when: "the promise is fulfilled"
	promise.onNext "test"

	then: "the promise is invoked without the accepted value"
	!after.get()
	after.isTerminated()
	after.isSuccess()

	when: "the promise is rejected"
	promise = Promise.ready()
	after = Promise.ready()
	promise.after().subscribe(after)

	promise.onError new Exception()

	then: "the promise is invoked with the rejecting value"
	after.isTerminated()
	after.isError()
	after.reason().class == Exception
  }

  def "An onError consumer is called when added to an already-rejected promise"() {
	given: "a rejected Promise"
	def failure = new Exception()
	def promise = Promise.error(failure)

	when: "an doOnError consumer is added"
	def acceptedValue

	promise.doOnError { v -> acceptedValue = v
	}.to(Promise.prepare())
	println promise.debug()

	then: "the consumer is invoked with the rejecting value"
	acceptedValue == failure
  }

  def "When getting a rejected promise's value the exception that the promise was rejected with is thrown"() {
	given: "a rejected Promise"
	def failure = new Exception()
	def promise = Promise.error(failure)

	when: "getting the promise's value"
	promise.get()

	then: "the error that the promise was rejected with is thrown"
	thrown(Exception)
  }

  def "A fulfilled promise's value is returned by get"() {
	given: "a fulfilled Promise"
	def promise = Promise.success('test')

	when: "getting the promise's value"
	def value = promise.get()

	then: "the value used to fulfil the promise is returned"
	value == 'test'
  }

  def "A promise can be fulfilled with null"() {
	given: "a promise"
	def promise = Promise.<Object> ready()

	when: "the promise is fulfilled with null"
	promise.onNext null

	then: "the promise has completed"
	promise.isTerminated()
  }

  def "A function can be used to map a Promise's value when it's fulfilled"() {
	given: "a promise with a mapping function"
	def promise = Promise.<Integer> ready()
	def mappedPromise = promise.map { it * 2 }

	when: "the original promise is fulfilled"
	promise.onNext 1

	then: "the mapped promise is fulfilled with the mapped value"
	mappedPromise.get() == 2
  }

  def "A map many can be used to bind to another Promise and compose asynchronous results "() {
	given: "a promise with a map many function"
	def promise = Promise.<Integer> ready()
	def mappedPromise = promise.then { Promise.success(it + 1) }

	when: "the original promise is fulfilled"
	println promise.debug()
	promise.onNext 1
	println promise.debug()

	then: "the mapped promise is fulfilled with the mapped value"
	mappedPromise.get() == 2
  }

  def "A function can be used to map an already-fulfilled Promise's value"() {
	given: "a fulfilled promise with a mapping function"
	def promise = Promise.success(1)

	when: "a mapping function is added"
	def mappedPromise = promise.map { it * 2 }

	then: "the mapped promise is fulfilled with the mapped value"
	mappedPromise.get() == 2
  }

  def "An onSuccess consumer registered via then is called when the promise is fulfilled"() {
	given: "A promise with an doOnSuccess consumer registered using then"
	Promise<String> promise = Promise.<String> ready()
	def value = null
	promise.doOnSuccess { value = it }.to(Promise.prepare())

	when: "The promise is fulfilled"
	promise.onNext 'test'

	then: "the consumer is called"
	value == 'test'
  }

  def "An onError consumer registered via then is called when the promise is rejected"() {
	given: "A promise with an doOnError consumer registered using then"
	Promise<String> promise = Promise.<String> ready()
	def value
	promise.doOnSuccess {}.doOnError { value = it }.to(Promise.prepare())

	when: "The promise is rejected"
	def e = new Exception()
	promise.onError e

	then: "the consumer is called"
	value == e
  }

  def "An onSuccess consumer registered via then is called when the promise is already fulfilled"() {
	given: "A promise that has been fulfilled"
	def promise = Promise.success('test')

	when: "An doOnSuccess consumer is registered via then"
	def value
	promise.doOnSuccess { value = it }.to(Promise.prepare())

	then: "The consumer is called"
	value == 'test'
  }

  def "When a promise is fulfilled, if a mapping function throws an exception the mapped promise is rejected"() {
	given: "a promise with a filter that throws an error"
	Promise<String> promise = Promise.<String> prepare()
	def e = new RuntimeException()
	def mapped = Promise.ready()
	promise.map { throw e }.subscribe(mapped)

	when: "the promise is fulfilled"
	promise.onNext 2
	mapped.request(1)

	then: "the mapped promise is rejected"
	mapped.error
  }

  def "When a promise is already fulfilled, if a mapping function throws an exception the mapped promise is rejected"() {
	given: "a fulfilled promise"
	def promise = Promise.success(1)

	when: "a mapping function that throws an error is added"
	def e = new RuntimeException()
	def mapped = Promise.prepare()
	promise.map { throw e }.subscribe(mapped)

	then: "the mapped promise is rejected"
	mapped.error
  }

  def "An IllegalStateException is thrown if an attempt is made to fulfil a fulfilled promise"() {
	given: "a fulfilled promise"
	def promise = Promise.<Integer> ready()

	when: "an attempt is made to fulfil it"
	promise.onNext 1
	promise.onNext 1

	then: "an CancelException is thrown"
	thrown(CancelException)
  }

  def "An IllegalStateException is thrown if an attempt is made to reject a rejected promise"() {
	given: "a rejected promise"
	Promise promise = Promise.ready()

	when: "an attempt is made to fulfil it"
	promise.onError new Exception()
	promise.onError new Exception()

	then: "an IllegalStateException is thrown"
	thrown(ReactorFatalException)
  }

  def "An IllegalStateException is thrown if an attempt is made to reject a fulfilled promise"() {
	given: "a fulfilled promise"
	def promise = Promise.ready()

	when: "an attempt is made to fulfil it"
	promise.onNext 1
	promise.onError new Exception()

	then: "an IllegalStateException is thrown"
	thrown(ReactorFatalException)
  }

  def "Multiple promises can be combined"() {
	given: "two fulfilled promises"
	def bc1 = Broadcaster.<Integer> create();
	def promise1 = bc1.observe { println 'hey' + it }.promise()
	def bc2 = Promise.<Integer> ready()
	def promise2 = bc2.stream().log().promise()

	when: "a combined promise is first created"
	def combined = Mono.when(promise1, promise2).to(Promise.prepare())

	then: "it is pending"
	combined.pending

	when: "the first promise is fulfilled"
	println promise1.debug()
	bc1.onNext 1
	println promise1.debug()

	then: "the combined promise is still pending"
	combined.pending

	when: "the second promise if fulfilled"
	bc2.onNext 2

	println combined.debug()

	then: "the combined promise is fulfilled with both values"
	combined.get().t1 == 1
	combined.get().t2 == 2
	combined.success
  }

  def "A combined promise is rejected once any of its component promises are rejected"() {
	given: "two unfulfilled promises"
	def promise1 = Promise.<Integer> ready()
	def promise2 = Promise.<Integer> ready()

	when: "a combined promise is first created"
	def combined = Mono.when(promise1, promise2).to(Promise.prepare())

	then: "it is pending"
	combined.pending

	when: "a component promise is rejected"
	promise1.onError new Exception()

	then: "the combined promise is rejected"
	combined.error
  }

  def "A combined promise is immediately fulfilled if its component promises are already fulfilled"() {
	given: "two fulfilled promises"
	def promise1 = Promise.success(1)
	def promise2 = Promise.success(2)

	when: "a combined promise is first created"
	def combined = Promise.prepare()
	Mono.when(promise1, promise2).subscribe(combined)
	combined.get()

	then: "it is fulfilled"
	combined.success
	combined.get().t1 == 1
	combined.get().t2 == 2

	when: "promises are supplied"
	promise1 = Mono.fromCallable { '1' }
	promise2 = Mono.fromCallable { '2' }
	combined = Promise.prepare()
	Mono.when(promise1, promise2).subscribe(combined)

	then: "it is fulfilled"
	combined.success
	combined.get().t1 == '1'
	combined.get().t2 == '2'

  }

  def "A combined promise through 'any' is fulfilled with the first component result when using synchronously"() {
	given: "two fulfilled promises"
	def promise1 = Promise.success(1)
	def promise2 = Promise.success(2)

	when: "a combined promise is first created"
	def combined = Promise.prepare()
	Mono.any(promise1, promise2).subscribe(combined)

	then: "it is fulfilled"
	combined.get() == 1
	combined.success
  }

  def "A combined promise through 'any' is fulfilled with the first component result when using asynchronously"() {
	given: "two fulfilled promises"
	def ioGroup = Processors.ioGroup("promise-task", 8, 2)
	def promise1 = Mono.fromCallable { sleep(10000); 1 }.publishOn(ioGroup)
	def promise2 = Mono.fromCallable { sleep(325); 2 }.publishOn(ioGroup)


	when: "a combined promise is first created"
	def combined =  Mono.any(promise1, promise2).to(Promise.prepare())

	then: "it is fulfilled"
	combined.awaitSuccess(3205, TimeUnit.MILLISECONDS)
	combined.get() == 2
  }

  def "A combined promise is immediately rejected if its component promises are already rejected"() {
	given: "two rejected promises"
	def promise1 = Promise.error(new Exception())
	def promise2 = Promise.error(new Exception())

	when: "a combined promise is first created"
	def combined = Promise.ready()
	Mono.when(promise1, promise2).subscribe(combined)
	println promise1.debug()
	println promise2.debug()
	println combined.debug()

	then: "it is rejected"
	thrown Exception
  }

  def "A single promise can be 'combined'"() {
	given: "one unfulfilled promise"
	def promise1 = Promise.ready()

	when: "a combined promise is first created"
	def combined = Promise.ready()
	Mono.when([promise1]).subscribe(combined)

	then: "it is pending"
	combined.pending

	when: "the first promise is fulfilled"
	println promise1.debug()
	promise1.onNext 1

	then: "the combined promise is fulfilled"
	combined.await(1, TimeUnit.SECONDS) == [1]
	combined.success
  }

  def "A promise can be fulfilled with a Supplier"() {
	when: "A promise configured with a supplier"
	def promise = Mono.fromCallable { 1 }

	then: "it is fulfilled"
	promise.get() == 1
  }

  def "A promise with a Supplier that throws an exception is rejected"() {
	when: "A promise configured with a supplier that throws an error"
	def promise = Mono.fromCallable { throw new RuntimeException() }
	promise.get()

	then: "it is rejected"
	thrown RuntimeException
  }

  def "A filtered promise is not fulfilled if the filter does not allow the value to pass through"() {
	given: "a promise with a filter that only accepts even values"
	def promise = Promise.ready()
	def filtered = promise.stream().filter { it % 2 == 0 }.next()

	when: "the promise is fulfilled with an odd value"
	promise.onNext 1

	then: "the filtered promise is not fulfilled"
	!filtered.get()
  }

  def "A filtered promise is fulfilled if the filter allows the value to pass through"() {
	given: "a promise with a filter that only accepts even values"
	def promise = Promise.ready()
	promise.stream().filter { it % 2 == 0 }.next()

	when: "the promise is fulfilled with an even value"
	promise.onNext 2

	then: "the filtered promise is fulfilled"
	promise.success
	promise.get() == 2
  }

  def "If a filter throws an exception the filtered promise is rejected"() {
	given: "a promise with a filter that throws an error"
	def promise = Promise.ready()
	def e = new RuntimeException()
	def filteredPromise = promise.stream().filter { throw e }.next()

	when: "the promise is fulfilled"
	promise.onNext 2
	filteredPromise.get()

	then: "the filtered promise is rejected"
	thrown RuntimeException
  }

  def "If a promise is already fulfilled with a value accepted by a filter the filtered promise is fulfilled"() {
	given: "a promise that is already fulfilled with an even value"
	def promise = Promise.success(2)

	when: "the promise is filtered with a filter that only accepts even values"
	def v = promise.stream().filter { it % 2 == 0 }.next().get()

	then: "the filtered promise is fulfilled"
	promise.get() == v
	promise.success
  }

  def "If a promise is already fulfilled with a value rejected by a filter, the filtered promise is not fulfilled"() {
	given: "a promise that is already fulfilled with an odd value"
	def promise = Promise.success(1)

	when: "the promise is filtered with a filter that only accepts even values"
	def v = promise.stream().filter { it % 2 == 0 }.next().get()

	then: "the filtered promise is not fulfilled"
	!v
  }

  def "Errors stop compositions"() {
	given: "a promise"
	def p1 = Promise.<String> ready()

	final latch = new CountDownLatch(1)

	when: "p1 is consumed by p2"
	Promise p2 = p1.doOnSuccess({ Integer.parseInt it }).
			doOnError{ latch.countDown() }.
			map { println('not in log'); true }.to(Promise.prepare())

	and: "setting a value"
	p1.onNext 'not a number'
	p2.await(1, TimeUnit.SECONDS)

	then: 'No value'
	thrown(RuntimeException)
	latch.count == 0
  }

  def "Can poll instead of await to automatically handle InterruptedException"() {
	given: "a promise"
	def p1 = Promise.<String> ready()

	when: "p1 is consumed by p2"
	def p2 = p1
			.dispatchOn(Processors.singleGroup('test'))
			.map {
	  println Thread.currentThread();
	  sleep(3000);
	  Integer.parseInt it
	}

	and: "setting a value"
	p1.onNext '1'
	println "emitted"
	 p2.get(1, TimeUnit.SECONDS)

	then: 'No value'
	thrown CancelException

	when: 'polling undefinitely'
	def v = p2.get()

	then: 'Value!'
	v
  }

}

