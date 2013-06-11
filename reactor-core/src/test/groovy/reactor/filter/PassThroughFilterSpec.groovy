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

class PassThroughFilterSpec extends Specification {

	def "When items are filtered they are returned as is"() {
		given: "A pass-through filter"
		def filter = new PassThroughFilter()

		when: "items are filtered"
		def items = ['a', 'b', 'c']
		def filteredItems = filter.filter items, null

		then: "they are returned as-is"
		items == filteredItems
	}

	def "When null items are filtered an IllegalStateException is thrown"() {
		given: "A pass-through filter"
		def filter = new PassThroughFilter()

		when: "null items are filtered"
		filter.filter null, null

		then: "an IllegalArgumentException is thrown"
		thrown(IllegalArgumentException)
	}
}
