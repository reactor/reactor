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



package reactor.core

import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static reactor.GroovyTestUtils.supplier

/**
 * @author Stephane Maldini
 */
class ComposableSpec extends Specification {

	def "Composable merge handling"() {

		when: "we create a series of Composable"
		def c1 = R.compose supplier { 'test' } get()
		def c2 = R.compose supplier { 'test2' } get()
		def c3 = R.compose supplier { 'test3' } get()
		def result = R.compose c1, c2, c3 get()

		then:
		result.await(1, TimeUnit.SECONDS) == ['test', 'test2', 'test3']
		c1.get() == 'test'
		c2.get() == 'test2'
		c3.get() == 'test3'

	}

}

