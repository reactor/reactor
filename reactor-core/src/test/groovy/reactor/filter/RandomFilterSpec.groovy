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

package reactor.filter

import spock.lang.Specification

class RandomFilterSpec extends Specification {

	def "When items are filtered a single randomly selected item is returned"() {
		given: "A random filter"
		def filter = new RandomFilter()

		when: "items are filtered"
		def items = ['a', 'b', 'c']
		def filteredItems = filter.filter items, null

		then: "a single item is returned"
		filteredItems.size() == 1
		items.contains filteredItems[0]
	}

	def "When null items are filtered an IllegalStateException is thrown"() {
		given: "A random filter"
		def filter = new RandomFilter()

		when: "null items are filtered"
		filter.filter null, null

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
