/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.alloc

import reactor.core.state.Recyclable
import reactor.fn.Supplier
import spock.lang.Specification

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 */
class AllocatorsSpec extends Specification {

	def "a ReferenceCountingAllocator should properly count references"() {

		given: "references and a thread pool"
			def threadPool = Executors.newCachedThreadPool()
			def pool = new ReferenceCountingAllocator(4, {
				new Recyclable() {
					@Override
					void recycle() {
					}
				}
			} as Supplier<Object>)
			def refs = (1..4).collect {
				pool.allocate()
			}
			def latch = new CountDownLatch(refs.size())

		when: "references are retained from other threads"
			refs.each { ref ->
				threadPool.submit(new ReferenceCounter(latch, ref, 1))
			}
			latch.await(5, TimeUnit.SECONDS)

		then: "references were all retained"
			refs.findAll { ref -> ref.referenceCount != 2 }.isEmpty()

		when: "references are released"
			latch = new CountDownLatch(refs.size())
			refs.each { ref ->
				threadPool.submit(new ReferenceCounter(latch, ref, -2))
			}
			latch.await(5, TimeUnit.SECONDS)

		then: "references were all released"
			refs.findAll { ref -> ref.referenceCount != 0 }.isEmpty()

		when: "the allocator pool is expanded"
			refs = (1..8).collect { pool.allocate() }
			def refToFind = refs[0]

		then: "the pool was expanded"
			refs.findAll { ref -> ref.referenceCount == 1 }.size() == 8

		and: "the references are all unique"
			refs.findAll { ref -> ref == refToFind }.size() == 1

	}

	def "Allocators can be provided by Type"() {

		given: "a generic type"
			def type = fromTypeRef(new TypeReference<Generic<String>>() {})
			def allocators = [:]
			allocators[type] = new ReferenceCountingAllocator<Generic<String>>(new Supplier<Generic<String>>() {
				@Override
				Generic<String> get() {
					return new Generic(data: "Hello World!")
				}
			})

		when: "a pool is requested"
			def pool = allocators[fromTypeRef(new TypeReference<Generic<String>>() {})]

		then: "a pool was returned"
			pool

	}

	class Generic<T> implements Recyclable {
		T data

		@Override
		void recycle() {
			data = null
		}
	}

	class ReferenceCounter implements Runnable {
		CountDownLatch latch
		Reference ref
		int delta

		ReferenceCounter(CountDownLatch latch, Reference ref, int delta) {
			this.latch = latch
			this.ref = ref
			this.delta = delta;
		}

		@Override
		void run() {
			if (delta > 0) {
				ref.retain(delta)
			} else {
				ref.release(Math.abs(delta))
			}
			latch.countDown()
		}
	}

	static <T> Type fromGenericType(Class<T> type) {
		return ((ParameterizedType) type.getGenericInterfaces()[0]).getActualTypeArguments()[0];
	}

	static <T> Type fromTypeRef(TypeReference<T> typeRef) {
		return fromGenericType(typeRef.getClass());
	}

	interface TypeReference<T> {
	}
}
