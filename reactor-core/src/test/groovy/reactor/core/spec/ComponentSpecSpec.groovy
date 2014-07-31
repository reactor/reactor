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
package reactor.core.spec

import reactor.event.dispatch.TraceableDelegatingDispatcher
import reactor.function.support.Tap
import reactor.rx.Promise
import reactor.rx.Stream
import reactor.rx.spec.Promises
import reactor.rx.spec.Streams
import spock.lang.Specification

/**
 * @author Stephane Maldini
 */
class ComponentSpecSpec extends Specification {

	def "Reactor correctly built"() {

		when:
			"we create a plain Reactor"
			def reactor = Reactors.reactor().synchronousDispatcher().get()

		then:
			reactor.core.Reactor.isAssignableFrom(reactor.class)
	}

	def "Tracing is enabled"() {

		when:
			"a traceable Reactor is created"
			def reactor = Reactors.reactor().synchronousDispatcher().traceEventPath().get()

		then:
			"Reactor uses traceable components"
			reactor.getDispatcher() instanceof TraceableDelegatingDispatcher
	}

	def "Stream correctly built"() {

		when:
			"we create a plain Composable"
			Stream composable = Streams.<String>config().synchronousDispatcher().get()
			Tap<String> tap = composable.tap()
			composable.broadcastNext('test')

		then:
			Stream.isAssignableFrom(composable.class)
			tap.get() == 'test'

	}

	def "Promise correctly built"() {

		when:
			"we create a plain Promise"
			Promise promise = Promises.<String>config().success('test').synchronousDispatcher().get()

		then:
			Promise.isAssignableFrom(promise.class)
			promise.get() == 'test'
	}

	def "Deferred Promise correctly built"() {

		when:
			"we create a plain Promise"
			Promise promise = Promises.<String>defer()
			promise.onNext 'test'

		then:
			promise.get() == 'test'
	}

}

