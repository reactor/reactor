/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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



package reactor.filter

import spock.lang.Specification

class RoundRobinFilterSpec extends Specification {

	def "When items are filtered multiple times a single item, selected using a round-robin algorithm, is returned"() {
		given: "A round robin filter and a list of three items"
		def filter = new RoundRobinFilter()
	  def items = ['a', 'b', 'c']

		when: "items are filtered"
		def filteredItems = filter.filter items, "key"

		then: "the first item is returned"
		filteredItems.size() == 1
		filteredItems[0] == 'a'

		when: "items are filtered a second time"
		filteredItems = filter.filter items, "key"

		then: "the second item is returned"
		filteredItems.size() == 1
		filteredItems[0] == 'b'

		when: "items are filtered a third time"
		filteredItems = filter.filter items, "key"

		then: "the third item is returned"
		filteredItems.size() == 1
		filteredItems[0] == 'c'

		when: "items are filtered a fourth time"
		filteredItems = filter.filter items, "key"

		then: "the first item is returned"
		filteredItems.size() == 1
		filteredItems[0] == 'a'

		when: "items are filtered with a different key"
		filteredItems = filter.filter items, "new-key"

		then: "the first item is returned"
	}

	def "When null items are filtered an IllegalStateException is thrown"() {
		given: "A round robin filter"
		def filter = new RoundRobinFilter()

		when: "null items are filtered"
		filter.filter null, null

		then: "an IllegalArgumentException was thrown"
		thrown(IllegalArgumentException)
	}

	def "When a null key is provided an IllegalStateException is thrown"() {
		given: "A round robin filter"
		def filter = new RoundRobinFilter()

		when: "a null key is provided"
		filter.filter(['a'], null)

		then: "an IllegalArgumentException was thrown"
		thrown(IllegalArgumentException)
	}

	def "When an empty list of items are filtered, an empty list is returned"() {
		given: "A random filter"
		def filter = new RandomFilter()

		when: "an empty list of items is filtered"
		def filteredItems = filter.filter([], null)

		then: "an empty list is returned"
		filteredItems.empty
	}
}
