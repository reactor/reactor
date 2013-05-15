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

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.Context
import reactor.core.Reactor
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class FactoryBeanSpec extends Specification {

	def "Reactors can be injected"() {

		given: "A configuration containing a ReactorFactoryBean"
		def ctx = new AnnotationConfigApplicationContext()
		ctx.register(ReactorInjectionTestConfig)
		ctx.refresh()

		when: "A Reactor is injected"
		def bean = ctx.getBean(ReactorAwareBean)

		then: "It should not be null and should have an id"
		null != bean
		null != bean.reactor.id

		when: "Another Reactor is requested from the context"
		def reactor = ctx.getBean(Reactor)

		then: "The ids should differ"
		null != reactor
		reactor.id != bean.reactor.id

		when: "the Dispatcher is checked"
		def d = reactor.getDispatcher()

		then: "it should be the sync Dispatcher"
		d == Context.synchronousDispatcher()

	}

}

class ReactorAwareBean {
	@Autowired Reactor reactor
}

@Configuration
class ReactorInjectionTestConfig {

	@Bean
	ReactorFactoryBean reactors() {
		return new ReactorFactoryBean().setDispatcher(Context.synchronousDispatcher())
	}

	@Bean
	ReactorAwareBean reactorAwareBean() {
		return new ReactorAwareBean()
	}

}