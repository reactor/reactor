package reactor.core.alloc

import reactor.core.alloc.factory.FactoryAllocator
import reactor.core.alloc.factory.NoArgConstructorFactory
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class FactoryAllocatorSpec extends Specification {

	def "factory can provide objects from a delegate"() {

		given: "a factory for objects and an allocator"
			def delegate = new NoArgConstructorFactory<Pojo>(Pojo)
			def pool = new FactoryAllocator<Pojo>(1024, delegate)

		when: "items are requested from the pool"
			def items = []
			(1..512).each { i ->
				items << pool.get()
			}

		then: "the pool was filled with unique items"
			items.size() == 512
			items.findAll { it == null }.isEmpty()
			items.findAll { it.equals(items[0]) }.size() == 1

		when: "more items are requested from the pool than were originally created"
			(1..1024).each { i ->
				items << pool.get()
			}

		then: "more unique items were created internally"
			items.size() == 1536
			items.findAll { it == null }.isEmpty()
			items.findAll { it.equals(items[0]) }.size() == 1

	}

	static class Pojo {}

}
