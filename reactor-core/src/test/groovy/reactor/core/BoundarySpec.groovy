package reactor.core

import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.function.Consumer
import reactor.function.support.Boundary
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static reactor.GroovyTestUtils.$

/**
 * @author Jon Brisbin
 */
class BoundarySpec extends Specification {

	def "A Boundary can wait on multiple Consumers"() {

		given: "A Boundary with multiple Consumers bound to it"
		def r = Reactors.reactor().
				env(new Environment()).
				dispatcher(Environment.THREAD_POOL).
				get()
		def boundary = new Boundary()
		def hellos = []
		(1..5).each {
			r.on($("test.$it"), boundary.bind({ Event<String> ev ->
				hellos << ev.data
			} as Consumer<Event<String>>))
		}

		when: "Consumers are triggered"
		(1..5).each {
			r.notify("test.$it".toString(), Event.wrap("Hello World!"))
		}

		then: "Boundary should block until all 5 have counted down"
		boundary.await(5, TimeUnit.SECONDS)

	}

}
