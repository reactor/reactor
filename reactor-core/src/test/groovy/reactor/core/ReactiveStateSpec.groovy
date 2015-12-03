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

import reactor.core.support.ReactiveStateUtils
import spock.lang.Specification

import static reactor.Processors.emitter
import static reactor.Processors.singleGroup
import static reactor.Publishers.*
import static reactor.core.subscriber.SubscriberFactory.unbounded

/**
 * @author Stephane Maldini
 */
class ReactiveStateSpec extends Specification {

  def "Scan reactive streams"() {

	when: "Iterable publisher of 1000 to read queue"

	def pub1 = map(from(1..1000), { d -> d })
	def pub2 = map(from(1..123), { d -> d })

	def t = ReactiveStateUtils.scan(pub1)
	println t
	println t.nodes
	println t.edges


	then: "scan values correct"
	t.nodes
	t.edges

	when: "merged"

	def pub3 = merge(pub1, pub2)

	println "after merge"
	t = ReactiveStateUtils.scan(pub3)
	println t
	println t.nodes
	println t.edges

	then: "scan values correct"
	t.nodes
	t.edges

	when: "processors"

	def proc1 = emitter()
	def proc2 = emitter()
	def sub1 = unbounded()
	def sub2 = unbounded()
	def sub3 = unbounded()
	def group = singleGroup().get()
	proc1.subscribe(sub1)
	group.subscribe(sub2)
	proc1.subscribe(sub3)
	proc1.subscribe(group)
	def zip = zip(pub3, proc2)

	t = ReactiveStateUtils.scan(zip)
	println t
	println t.nodes
	println t.edges

	zip.subscribe(group)

	println "after zip/subscribe"
	t = ReactiveStateUtils.scan(sub1)
	println t
	println t.nodes
	println t.edges

	then: "scan values correct"
	t.nodes

	cleanup:
	group.onComplete()
  }

}
