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
package reactor.nexus.pylon

import reactor.io.net.ReactiveNet
import spock.lang.Ignore
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

/**
 * @author Stephane Maldini
 */
@Ignore
class PylonSpec extends Specification {

	def "http responds to requests from clients"() {
		given: "a simple Console"

			//Listen on localhost using default impl (Netty) and assign a global codec to receive/reply String data
		def nexus = ReactiveNet.nexus()

		def latch = new CountDownLatch(1)

		when: "the server is prepared"

			//prepare post request consumer on /test/* and capture the URL parameter "param"
		nexus.server.get('/exit') { req ->
				latch.countDown()
			}

		then: "the server was started"
		nexus?.startAndAwait()
			latch.await()


		cleanup: "the client/server where stopped"
			//note how we order first the client then the server shutdown
		nexus?.shutdown()
	}

}