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





package reactor.spring.factory.dynamic

import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.Environment
import reactor.core.dynamic.DynamicReactor
import reactor.core.dynamic.annotation.Dispatcher
import reactor.function.Consumer
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class DynamicFactoryBeanSpec extends Specification {

	def "DynamicReactors are injectable and events are emitted inside them"() {

		given: "a standard AppicationContext"
		def appCtx = new AnnotationConfigApplicationContext(TestReactorConfig)
		def r = appCtx.getBean(TestReactor)
		def latch = new CountDownLatch(1)

		when: "the interface API is called"
		r.onTest({ s ->
			latch.countDown()
		} as Consumer<String>)
		r.notifyTest("Hello World!")
		latch.await(5, TimeUnit.SECONDS)

		then: "the latch has been counted down"
		latch.count == 0

	}

}

@Dispatcher(Environment.THREAD_POOL)
interface TestReactor extends DynamicReactor {
	TestReactor onTest(Consumer<String> consumer)

	TestReactor notifyTest(String s)
}

@Configuration
class TestReactorConfig {
	@Bean
	DynamicReactorFactoryBean<TestReactor> testReactorFactoryBean() {
		return new DynamicReactorFactoryBean<TestReactor>(new Environment(), TestReactor)
	}
}