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

package reactor.core

import reactor.core.publisher.FluxLift
import reactor.fn.BiConsumer
import spock.lang.Specification

import static reactor.Publishers.*

/**
 * @author Stephane Maldini
 */
class PublishersSpec extends Specification {

  def "Read Queues from Publishers"() {

	given: "Iterable publisher of 1000 to read queue"
	def pub = from(1..1000)
	def queue = toReadQueue(pub)

	when: "read the queue"
	def v = queue.take()
	def v2 = queue.take()
	997.times {
	  queue.poll()
	}

	def v3 = queue.take()

	then: "queues values correct"
	v == 1
	v2 == 2
	v3 == 1000
  }

  def "Error handling with onErrorReturn"() {

	given: "Iterable publisher of 1000 to read queue"
	def pub = FluxLift.lift(from(1..1000), { d, s ->
	  if (d == 3) {
		throw new Exception('test')
	  }
	  s.onNext(d)
	} as BiConsumer)

	when: "read the queue"
	def q = toReadQueue(onErrorReturn(pub, 100000))
	def res = []
	q.drainTo(res)

	then: "queues values correct"
	res == [1, 2, 100000]
  }

  def "Error handling with onErrorResume"() {

	given: "Iterable publisher of 1000 to read queue"
	def pub = FluxLift.lift(from(1..1000), { d, s ->
	  if (d == 3) {
		throw new Exception('test')
	  }
	  s.onNext(d)
	} as BiConsumer)

	when: "read the queue"
	def q = toReadQueue(switchOnError(pub, from(9999..10002)))
	def res = []
	q.drainTo(res)

	then: "queues values correct"
	res == [1, 2, 9999, 10000, 10001, 10002]
  }

}
