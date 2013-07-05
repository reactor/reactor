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









package reactor.core

import reactor.P
import reactor.R
import reactor.S
import reactor.fn.support.Tap
import spock.lang.Specification

/**
 * @author Stephane Maldini
 */
class ComponentSpecSpec extends Specification {

	def "Reactor correctly built"() {

		when: "we create a plain Reactor"
		def reactor = R.reactor().sync().get()

		then:
		Reactor.isAssignableFrom(reactor.class)
	}

	def "Composable correctly built"() {

		when: "we create a plain Composable"
		Deferred composable = S.defer().sync().get()
		Tap<String> tap = composable.compose().tap()
		composable.accept('test')

		then:
		Deferred.isAssignableFrom(composable.class)
		tap.get() == 'test'

	}

	def "Promise correctly built"() {

		when: "we create a plain Promise"
		Promise promise = P.success('test').sync().get()

		then:
		Promise.isAssignableFrom(promise.class)
		promise.get() == 'test'
	}

	def "Deferred Promise correctly built"() {

		when: "we create a plain Promise"
		Deferred promise = P.defer().get()
		promise.accept 'test'

		then:
		Deferred.isAssignableFrom(promise.class)
		promise.compose().get() == 'test'
	}

}

