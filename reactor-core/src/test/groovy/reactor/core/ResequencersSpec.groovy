package reactor.core

import reactor.function.Consumer
import reactor.function.support.Resequencer
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class ResequencersSpec extends Specification {

	def "re-orders events in the proper order"() {

		given:
			"a Resequencer"
			def nums = []
			def outOfOrderNums = [2, 3, 1, 5, 4] as Long[]
			long next = 1
			def seq = new Resequencer<Long>({ l -> nums << next } as Consumer<Long>)

		when:
			"slots are allocated and claimed"
			outOfOrderNums.each {
				seq.next()
			}
			outOfOrderNums.each {
				seq.accept(it, it)
			}

		then:
			"slots were re-ordered"
			nums == outOfOrderNums.sort()

	}

}
