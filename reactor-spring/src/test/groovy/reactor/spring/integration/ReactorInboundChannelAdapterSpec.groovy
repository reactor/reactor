package reactor.spring.integration

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import reactor.core.Reactor
import reactor.fn.Event
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 */
@ContextConfiguration(locations = "classpath*:reactor/spring/integration/inbound.xml")
class ReactorInboundChannelAdapterSpec extends Specification {

	@Autowired
	CountDownLatch latch
	@Autowired
	Reactor reactor

	def "Reactors can send messages to an SI Channel"() {
		when: "an event is sent to a Reactor"
		reactor.notify(Event.wrap("Hello World!"))
		latch.await(5, TimeUnit.SECONDS)

		then: "the latch was counted down"
		latch.count == 0
	}

}
