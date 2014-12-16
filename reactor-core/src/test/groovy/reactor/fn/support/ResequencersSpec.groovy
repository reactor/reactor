package reactor.fn.support

import reactor.fn.Consumer
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
			def seq = new Resequencer<Long>({ l -> nums << l } as Consumer<Long>)

		when:
			"slots are allocated and claimed"
			(1..5).each {
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
