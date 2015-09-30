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
package reactor.rx

import reactor.Processors
import reactor.bus.EventBus
import reactor.bus.selector.Selectors
import reactor.core.error.CancelException
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
		given:
			"a Promise with an onComplete Consumer"
			def promise = Promises.ready()
			def acceptedPromise

			promise.onComplete { self ->
				acceptedPromise = self
			}

		when:
			"the promise is rejected"
			promise.onError new Exception()

		then:
			"the consumer is invoked with the promise"
			acceptedPromise == promise
			promise.reason()
	}

	def "An onComplete consumer is called when added to an already-rejected promise"() {
		given:
			"a rejected Promise"
			def promise = Promises.<Object> error(new Exception())

		when:
			"an onComplete consumer is added"
			def acceptedPromise

			promise.onComplete { self ->
				acceptedPromise = self
			}

		then:
			"the consumer is invoked with the promise"
			acceptedPromise == promise
			promise.reason()
	}

	def "An onComplete consumer is called when a promise is fulfilled"() {
		given:
			"a Promise with an onComplete Consumer"
			def promise = Promises.ready()
			def acceptedPromise

			promise.onComplete { self ->
				acceptedPromise = self
			}

		when:
			"the promise is fulfilled"
			promise.onNext 'test'

		then:
			"the consumer is invoked with the promise"
			acceptedPromise == promise
			promise.success
	}

	def "An onComplete consumer is called when added to an already-fulfilled promise"() {
		given:
			"a fulfilled Promise"
			def promise = Promises.success('test')

		when:
			"an onComplete consumer is added"
			def acceptedPromise

			promise.onComplete { self ->
				acceptedPromise = self
			}

		then:
			"the consumer is invoked with the promise"
			acceptedPromise == promise
			promise.success
	}

	def "An onSuccess consumer is called when a promise is fulfilled"() {
		given:
			"a Promise with an onSuccess Consumer"
			def promise = Promises.ready()
			def acceptedValue

			promise.onSuccess { v ->
				acceptedValue = v
			}

		when:
			"the promise is fulfilled"
			promise.onNext 'test'

		then:
			"the consumer is invoked with the fulfilling value"
			acceptedValue == 'test'
	}

	def "An onSuccess consumer is called when added to an already-fulfilled promise"() {
		given:
			"a fulfilled Promise"
			def promise = Promises.success('test')

		when:
			"an onSuccess consumer is added"
			def acceptedValue

			promise.onSuccess { v ->
				acceptedValue = v
			}

		then:
			"the consumer is invoked with the fulfilling value"
			acceptedValue == 'test'
	}

	def "An onSuccess consumer can be added to an already-rejected promise"() {
		given:
			"a rejected Promise"
			def promise = Promises.error(new Exception())

		when:
			"an onSuccess consumer is added"
			def ex = null
			promise.onError { ex = it }

		then:
			"no error is thrown"
			ex in Exception
			notThrown Exception
	}

	def "An onError consumer can be added to an already-fulfilled promise"() {
		given:
			"a fulfilled Promise"
			def promise = Promises.success('test')

		when:
			"an onError consumer is added"
			promise.onSuccess {}

		then:
			"no error is thrown"
			notThrown Exception
	}

	def "An onError consumer is called when a promise is rejected"() {
		given:
			"a Promise with an onError Consumer"
			def promise = Promises.ready()
			def acceptedValue

			promise.onError { v ->
				acceptedValue = v
			}

		when:
			"the promise is rejected"
			def failure = new Exception()
			promise.onError failure

		then:
			"the consumer is invoked with the rejecting value"
			acceptedValue == failure
	}

	def "A promise can only listen to terminal states"() {
		given:
			"a Promise with an onError Consumer"
			def promise = Promises.ready()
			def after = promise.after()

		when:
			"the promise is fulfilled"
			promise.onNext "test"

		then:
			"the promise is invoked without the accepted value"
			after.isComplete()
			after.isSuccess()
			!after.get()

		when:
			"the promise is rejected"
			promise = Promises.ready()
			after = promise.after()

			promise.onError new Exception()

		then:
			"the promise is invoked with the rejecting value"
			after.isComplete()
			after.isError()
			after.reason().class == Exception
	}


	def "An onError consumer is called when added to an already-rejected promise"() {
		given:
			"a rejected Promise"
			def failure = new Exception()
			def promise = Promises.error(failure)

		when:
			"an onError consumer is added"
			def acceptedValue

			promise.onError { v ->
				acceptedValue = v
			}
			println promise.debug()

		then:
			"the consumer is invoked with the rejecting value"
			acceptedValue == failure
	}

	def "When getting a rejected promise's value the exception that the promise was rejected with is thrown"() {
		given:
			"a rejected Promise"
			def failure = new Exception()
			def promise = Promises.error(failure)

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
			def promise = Promises.success('test')

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
			def promise = Promises.<Object> ready()

		when:
			"the promise is fulfilled with null"
			promise.onNext null

		then:
			"the promise has completed"
			promise.isComplete()
	}

	def "An Observable can be used to consume a promise's value when it's fulfilled"() {
		given:
			"a promise with a consuming Observable"
			def promise = Promises.<Object> ready()
			def promise2 = Promises.<Object> ready()
			def bus = EventBus.create()

			bus.on(Selectors.$('key'), promise2)
			bus.notify(promise, 'key')

		when:
			"the promise is fulfilled"
			promise.onNext 'test'

		then:
			"the observable is notified"
			promise2.await()?.data == 'test'
	}

	def "An Observable can be used to consume the value of an already-fulfilled promise"() {
		given:
			"a fulfilled promise"
			def promise = Promises.success('test')
			def promise2 = Promises.<Object> ready()
			def bus = EventBus.create()

		when:
			"an Observable is added as a consumer"
			bus.on(Selectors.$('key'), promise2)
			bus.notify(promise, 'key')

		then:
			"the observable is notified"
			promise2.await()?.data == 'test'
	}

	def "A function can be used to map a Promise's value when it's fulfilled"() {
		given:
			"a promise with a mapping function"
			def promise = Promises.<Integer> ready()
			def mappedPromise = promise.map { it * 2 }

		when:
			"the original promise is fulfilled"
			promise.onNext 1

		then:
			"the mapped promise is fulfilled with the mapped value"
			mappedPromise.get() == 2
	}

	def "A map many can be used to bind to another Promise and compose asynchronous results "() {
		given:
			"a promise with a map many function"
			def promise = Promises.<Integer> ready()
			def mappedPromise = promise.flatMap { Promises.success(it + 1) }

		when:
			"the original promise is fulfilled"
			println promise.debug()
			promise.onNext 1
			println promise.debug()

		then:
			"the mapped promise is fulfilled with the mapped value"
			mappedPromise.get() == 2
	}

	def "A function can be used to map an already-fulfilled Promise's value"() {
		given:
			"a fulfilled promise with a mapping function"
			def promise = Promises.success(1)

		when:
			"a mapping function is added"
			def mappedPromise = promise.map { it * 2 }

		then:
			"the mapped promise is fulfilled with the mapped value"
			mappedPromise.get() == 2
	}

	def "An onSuccess consumer registered via then is called when the promise is fulfilled"() {
		given:
			"A promise with an onSuccess consumer registered using then"
			Promise<String> promise = Promises.<String> ready()
			def value = null
			promise.onSuccess { value = it }

		when:
			"The promise is fulfilled"
			promise.onNext 'test'

		then:
			"the consumer is called"
			value == 'test'
	}

	def "An onError consumer registered via then is called when the promise is rejected"() {
		given:
			"A promise with an onError consumer registered using then"
			Promise<String> promise = Promises.<String> ready()
			def value
			promise.onSuccess {}.onError { value = it }

		when:
			"The promise is rejected"
			def e = new Exception()
			promise.onError e

		then:
			"the consumer is called"
			value == e
	}

	def "An onSuccess consumer registered via then is called when the promise is already fulfilled"() {
		given:
			"A promise that has been fulfilled"
			def promise = Promises.success('test')

		when:
			"An onSuccess consumer is registered via then"
			def value
			promise.onSuccess { value = it }

		then:
			"The consumer is called"
			value == 'test'
	}

	def "When a promise is fulfilled, if a mapping function throws an exception the mapped promise is rejected"() {
		given:
			"a promise with a filter that throws an error"
			Promise<String> promise = Promises.<String> ready()
			def e = new RuntimeException()
			def mapped = promise.map { throw e }

		when:
			"the promise is fulfilled"
			promise.onNext 2

		then:
			"the mapped promise is rejected"
			mapped.error
	}

	def "When a promise is already fulfilled, if a mapping function throws an exception the mapped promise is rejected"() {
		given:
			"a fulfilled promise"
			def promise = Promises.success(1)

		when:
			"a mapping function that throws an error is added"
			def e = new RuntimeException()
			def mapped = promise.map { throw e }

		then:
			"the mapped promise is rejected"
			mapped.error
	}

	def "An IllegalStateException is thrown if an attempt is made to fulfil a fulfilled promise"() {
		given:
			"a fulfilled promise"
			def promise = Promises.<Integer> ready()

		when:
			"an attempt is made to fulfil it"
			promise.onNext 1
			promise.onNext 1

		then:
			"an CancelException is thrown"
			thrown(CancelException)
	}

	def "An IllegalStateException is thrown if an attempt is made to reject a rejected promise"() {
		given:
			"a rejected promise"
			Promise promise = Promises.ready()

		when:
			"an attempt is made to fulfil it"
			promise.onError new Exception()
			promise.onError new Exception()

		then:
			"an IllegalStateException is thrown"
			thrown(CancelException)
	}

	def "An IllegalStateException is thrown if an attempt is made to reject a fulfilled promise"() {
		given:
			"a fulfilled promise"
			def promise = Promises.ready()

		when:
			"an attempt is made to fulfil it"
			promise.onNext 1
			promise.onError new Exception()

		then:
			"an IllegalStateException is thrown"
			thrown(CancelException)
	}

	def "Multiple promises can be combined"() {
		given:
			"two fulfilled promises"
			def promise1 = Broadcaster.<Integer> create().observe { println 'hey' + it }.next()
			def promise2 = Promises.<Integer> ready()

		when:
			"a combined promise is first created"
			Promise combined = Promises.when(promise1, promise2)

		then:
			"it is pending"
			combined.pending

		when:
			"the first promise is fulfilled"
			println promise1.debug()
			promise1.onNext 1
			println promise1.debug()

		then:
			"the combined promise is still pending"
			combined.pending

		when:
			"the second promise if fulfilled"
			promise2.onNext 2

			println combined.debug()

		then:
			"the combined promise is fulfilled with both values"
			combined.success
			combined.get().t1 == 1
			combined.get().t2 == 2
	}

	def "A combined promise is rejected once any of its component promises are rejected"() {
		given:
			"two unfulfilled promises"
			def promise1 = Promises.<Integer> ready()
			def promise2 = Promises.<Integer> ready()

		when:
			"a combined promise is first created"
			def combined = Promises.when(promise1, promise2)

		then:
			"it is pending"
			combined.pending

		when:
			"a component promise is rejected"
			promise1.onError new Exception()

		then:
			"the combined promise is rejected"
			combined.error
	}

	def "A combined promise is immediately fulfilled if its component promises are already fulfilled"() {
		given:
			"two fulfilled promises"
			def promise1 = Promises.success(1)
			def promise2 = Promises.success(2)

		when:
			"a combined promise is first created"
			def combined = Promises.when(promise1, promise2)

		then:
			"it is fulfilled"
			combined.success
			combined.get().t1 == 1
			combined.get().t2 == 2

		when:
			"promises are supplied"
			promise1 = Promises.task { '1' }
			promise2 = Promises.task { '2' }
			combined = Promises.when(promise1, promise2)

		then:
			"it is fulfilled"
			combined.success
			combined.get().t1 == '1'
			combined.get().t2 == '2'

	}

	def "A combined promise through 'any' is fulfilled with the first component result when using synchronously"() {
		given:
			"two fulfilled promises"
			def promise1 = Promises.success(1)
			def promise2 = Promises.success(2)

		when:
			"a combined promise is first created"
			def combined = Promises.any(promise1, promise2)

		then:
			"it is fulfilled"
			combined.success
			combined.get() == 1
	}

	def "A combined promise through 'any' is fulfilled with the first component result when using asynchronously"() {
		given:
			"two fulfilled promises"
		def ioGroup = Processors.ioGroup("promise-task", 8, 2)
			def promise1 = Promises.task(ioGroup.get()){ sleep(10000); 1 }
			def promise2 = Promises.task(ioGroup.get()){ sleep(250); 2 }

		when:
			"a combined promise is first created"
			def combined = Promises.any(promise2, promise1)

		then:
			"it is fulfilled"
			combined.awaitSuccess(325, TimeUnit.MILLISECONDS)
			!promise1.complete
			combined.get() == 2
	}

	def "A combined promise is immediately rejected if its component promises are already rejected"() {
		given:
			"two rejected promises"
			def promise1 = Promises.error(new Exception())
			def promise2 = Promises.error(new Exception())

		when:
			"a combined promise is first created"
			def combined = Promises.when(promise1, promise2)
			println promise1.debug()
			println promise2.debug()
			println combined.debug()

		then:
			"it is rejected"
			combined.error
	}

	def "A single promise can be 'combined'"() {
		given:
			"one unfulfilled promise"
			def promise1 = Promises.ready()

		when:
			"a combined promise is first created"
			def combined = Promises.when([promise1])

		then:
			"it is pending"
			combined.pending

		when:
			"the first promise is fulfilled"
			println promise1.debug()
			promise1.onNext 1

		then:
			"the combined promise is fulfilled"
			combined.await(1, TimeUnit.SECONDS) == [1]
			combined.success
	}

	def "A promise can be fulfilled with a Supplier"() {
		when:
			"A promise configured with a supplier"
			def promise = Promises.task { 1 }

		then:
			"it is fulfilled"
			promise.await() == 1
			promise.success
	}

	def "A promise with a Supplier that throws an exception is rejected"() {
		when:
			"A promise configured with a supplier that throws an error"
			def promise = Promises.task { throw new RuntimeException() }
			promise.await()

		then:
			"it is rejected"
			thrown RuntimeException
	}

	def "A filtered promise is not fulfilled if the filter does not allow the value to pass through"() {
		given:
			"a promise with a filter that only accepts even values"
			def promise = Promises.ready()
			def filtered = promise.stream().filter { it % 2 == 0 }.next()

		when:
			"the promise is fulfilled with an odd value"
			promise.onNext 1

		then:
			"the filtered promise is not fulfilled"
			filtered.complete
			!filtered.get()
	}

	def "A filtered promise is fulfilled if the filter allows the value to pass through"() {
		given:
			"a promise with a filter that only accepts even values"
			def promise = Promises.ready()
			promise.stream().filter { it % 2 == 0 }.next()

		when:
			"the promise is fulfilled with an even value"
			promise.onNext 2

		then:
			"the filtered promise is fulfilled"
			promise.success
			promise.get() == 2
	}

	def "If a filter throws an exception the filtered promise is rejected"() {
		given:
			"a promise with a filter that throws an error"
			def promise = Promises.ready()
			def e = new RuntimeException()
			def filteredPromise = promise.stream().filter { throw e }.next()

		when:
			"the promise is fulfilled"
			promise.onNext 2

		then:
			"the filtered promise is rejected"
			filteredPromise.error
	}

	def "If a promise is already fulfilled with a value accepted by a filter the filtered promise is fulfilled"() {
		given:
			"a promise that is already fulfilled with an even value"
			def promise = Promises.success(2)

		when:
			"the promise is filtered with a filter that only accepts even values"
			promise.stream().filter { it % 2 == 0 }

		then:
			"the filtered promise is fulfilled"
			promise.success
			promise.get() == 2
	}

	def "If a promise is already fulfilled with a value rejected by a filter, the filtered promise is not fulfilled"() {
		given:
			"a promise that is already fulfilled with an odd value"
			def promise = Promises.success(1)

		when:
			"the promise is filtered with a filter that only accepts even values"
			def filtered = promise.stream().filter { it % 2 == 0 }.next()

		then:
			"the filtered promise is not fulfilled"
			filtered.complete
			!filtered.get()
	}

	def "Errors stop compositions"() {
		given:
			"a promise"
			def p1 = Promises.<String> ready()

			final latch = new CountDownLatch(1)

		when:
			"p1 is consumed by p2"
			Promise p2 = p1.onSuccess({ Integer.parseInt it }).stream().
					when(NumberFormatException, { latch.countDown() }).
					map { println('not in log'); true }.next()

		and:
			"setting a value"
			p1.onNext 'not a number'
			p2.await(1, TimeUnit.SECONDS)

		then:
			'No value'
			thrown(RuntimeException)
			latch.count == 0
	}

	def "Can poll instead of await to automatically handle InterruptedException"() {
		given:
			"a promise"
			def p1 = Promises.<String> ready()

		when:
			"p1 is consumed by p2"
			Promise p2 = p1
					.onSuccess({ Integer.parseInt it })
					.run(Processors.asyncGroup('test'))
					.map { sleep(3000); it }

		and:
			"setting a value"
			p1.onNext '1'
			p2.poll(1, TimeUnit.SECONDS)

		then:
			'No value'
			!p2.get()

		when:
			'polling undefinitely'
			p2.poll()

		then:
			'Value!'
			p2.get()
	}

}

