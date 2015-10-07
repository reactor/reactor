/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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


package reactor.core

import static reactor.Publishers.*
import spock.lang.Specification

/**
 * @author Stephane Maldini
 */
class PublishersSpec extends Specification {

	def "Read Queues from Publishers"() {

		given:
			"Iterable publisher of 1000 to read queue"
			def pub = from(1..1000)
			def queue = toReadQueue(pub)

		when:
			"read the queue"
			def v = queue.take()
			def v2 = queue.take()
		  997.times {
			  queue.poll()
		  }

		def v3 = queue.take()

		then:
			"queues values correct"
			v == 1
			v2 == 2
			v3 == 1000
	}

}
