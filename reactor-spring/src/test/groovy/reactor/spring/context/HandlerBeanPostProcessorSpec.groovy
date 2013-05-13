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

package reactor.spring.context

import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.Fn
import reactor.core.Reactor
import reactor.core.R
import reactor.core.Context
import reactor.fn.Event
import reactor.spring.context.annotation.On
import spock.lang.Specification

import static reactor.Fn.$

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class HandlerBeanPostProcessorSpec extends Specification {

	def "Annotated handler is wired to a Reactor"() {

		given: "an ApplicationContext with an annotated bean handler"
		def appCtx = new AnnotationConfigApplicationContext(AnnotatedHandlerConfig)
		def handlerBean = appCtx.getBean(HandlerBean)
		def reactor = appCtx.getBean(Reactor)

		when: "an Event is emitted onto the Reactor in context"
		reactor.notify('test', Fn.event("Hello World!"))

		then: "the method has been invoked"
		handlerBean.handled

		when: "the event is emitted on the root Reactor"
		handlerBean.handled = false
		R.notify('test', Fn.event("Hello World!"))
		Thread.sleep(250) // Naive way to make sure the task has been dispatched in the other thread

		then: "the method has been invoked"
		handlerBean.handled

	}

}

class HandlerBean {
	def handled = false

	@On(reactor = "@rootReactor", selector = "test")
	def handleTest() {
		handled = true
	}

	@On(selector = "test")
	def handleRootTest(Event<String> ev) {
		handled = (ev.data == "Hello World!")
	}
}

@Configuration
class AnnotatedHandlerConfig {

	@Bean
	Reactor rootReactor() {
		return R.create(true)
	}

	@Bean
	ConsumerBeanPostProcessor handlerBeanPostProcessor() {
		return new ConsumerBeanPostProcessor()
	}

	@Bean
	HandlerBean handlerBean() {
		return new HandlerBean()
	}

}
