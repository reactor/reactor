/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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





package reactor.core.dispatch

import reactor.bus.Event
import reactor.bus.filter.PassThroughFilter
import reactor.bus.registry.Registration
import reactor.bus.routing.ConsumerFilteringRouter
import reactor.bus.routing.ConsumerInvoker
import reactor.bus.selector.Selector
import reactor.fn.Consumer
import spock.lang.Specification

class ConsumerFilteringEventRouterSpec extends Specification {

	def "Basic event routing"() {
		def filter = new PassThroughFilter()
		def consumerInvoker = Mock(ConsumerInvoker)
		def completionConsumer = Mock(Consumer)
		def errorConsumer = Mock(Consumer)
		def consumer = Mock(Consumer)
		def event = new Event("data")

		given: "A consumer filtering event router"
			def eventRouter = new ConsumerFilteringRouter(filter, consumerInvoker)

		when: "An event is routed to a single registered consumer"
			Registration registration = Mock(Registration)
			registration.getObject() >> consumer
			registration.getSelector() >> Mock(Selector)

			eventRouter.route("key", event, [registration], completionConsumer, errorConsumer)

		then: "The consumerInvoker is called to invoke the consumer and the completionConsumer"
			1 * consumerInvoker.invoke(consumer, _, event)
			1 * completionConsumer.accept(event)
			0 * errorConsumer.accept(_)
	}

	def "Events are not routed to paused consumers"() {
		def filter = new PassThroughFilter()
		def consumerInvoker = Mock(ConsumerInvoker)
		def completionConsumer = Mock(Consumer)
		def errorConsumer = Mock(Consumer)
		def consumer = Mock(Consumer)
		def event = new Event("data")

		given: "A consumer filtering event router"
			def eventRouter = new ConsumerFilteringRouter(filter, consumerInvoker)

		when: "an event is routed to a paused consumer"
			Registration registration = Mock()
			registration.getObject() >> consumer
			registration.getSelector() >> Mock(Selector)
			registration.isPaused() >> true

			eventRouter.route("key", event, [registration], completionConsumer, errorConsumer)

		then: "the consumer invoker is only called to invoke the completionConsumer"
			0 * consumerInvoker.invoke(consumer, _, _, _)
			1 * completionConsumer.accept(event)
			0 * errorConsumer.accept(_)
	}

	def "Events are not routed to cancelled consumers"() {
		def filter = new PassThroughFilter()
		def consumerInvoker = Mock(ConsumerInvoker)
		def completionConsumer = Mock(Consumer)
		def errorConsumer = Mock(Consumer)
		def consumer = Mock(Consumer)
		def event = new Event("data")

		given: "A consumer filtering event router"
			def eventRouter = new ConsumerFilteringRouter(filter, consumerInvoker)

		when: "an event is routed to a paused consumer"
			Registration registration = Mock(Registration)
			registration.getObject() >> consumer
			registration.getSelector() >> Mock(Selector)
			registration.isCancelled() >> true

			eventRouter.route("key", event, [registration], completionConsumer, errorConsumer)

		then: "the consumer invoker is only called to invoke the completion consumer"
			0 * consumerInvoker.invoke(consumer, _, _, _)
			1 * completionConsumer.accept(event)
			0 * errorConsumer.accept(_)
	}

	def "Consumers configured to be cancelled after use are cancelled once they've been used"() {
		def filter = new PassThroughFilter()
		def consumerInvoker = Mock(ConsumerInvoker)
		def completionConsumer = Mock(Consumer)
		def errorConsumer = Mock(Consumer)
		def consumer = Mock(Consumer)
		def event = new Event("data")

		given: "A consumer filtering event router"
			def eventRouter = new ConsumerFilteringRouter(filter, consumerInvoker)
			Registration registration = Mock(Registration)

		when: "an event is routed to a cancel after use consumer"
			registration.getObject() >> consumer
			registration.getSelector() >> Mock(Selector)
			registration.isCancelAfterUse() >> true

			eventRouter.route("key", event, [registration], completionConsumer, errorConsumer)

		then: "the consumer invoker is called to invoke the consumer and completion consumer and the consumer is cancelled"
			1 * consumerInvoker.invoke(consumer, _, _)
			1 * completionConsumer.accept(event)
			1 * registration.cancel()
			0 * errorConsumer.accept(_)
	}

	def "An exception during event routing causes the errorConsumer to be invoked"() {
		def filter = new PassThroughFilter()
		def consumerInvoker = Mock(ConsumerInvoker)
		def completionConsumer = Mock(Consumer)
		def errorConsumer = Mock(Consumer)
		def consumer = Mock(Consumer)
		def event = new Event("data")

		given: "A consumer filtering event router"
			def eventRouter = new ConsumerFilteringRouter(filter, consumerInvoker)
			Registration registration = Mock(Registration)

		when: "event routing triggers an exception"
			registration.getObject() >> consumer
			registration.getSelector() >> Mock(Selector)

			consumerInvoker.invoke(_, _, _) >> { throw new Exception("failure") }

			eventRouter.route("key", event, [registration], completionConsumer, errorConsumer)

		then: "the error consumer is invoked"
			1 * errorConsumer.accept(_)
			1 * completionConsumer.accept(event)
	}

}
