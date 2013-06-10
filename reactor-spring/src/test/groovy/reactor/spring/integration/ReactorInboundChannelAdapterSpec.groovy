/*
 * Copyright (c) 2011-2013 the original author or authors.
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
