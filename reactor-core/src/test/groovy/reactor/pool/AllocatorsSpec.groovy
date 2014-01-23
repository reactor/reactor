package reactor.pool

import reactor.function.Supplier
import spock.lang.Specification

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

}
