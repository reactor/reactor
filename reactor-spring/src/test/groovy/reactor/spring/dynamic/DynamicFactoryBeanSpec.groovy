package reactor.spring.dynamic

import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.dynamic.annotation.DispatcherType
import reactor.core.dynamic.DynamicReactor
import reactor.core.dynamic.annotation.Dispatcher
import reactor.fn.Consumer
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
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

@Dispatcher(DispatcherType.THREAD_POOL)
interface TestReactor extends DynamicReactor {
	TestReactor onTest(Consumer<String> consumer)

	TestReactor notifyTest(String s)
}

@Configuration
class TestReactorConfig {
	@Bean
	DynamicReactorFactoryBean<TestReactor> testReactorFactoryBean() {
		return new DynamicReactorFactoryBean<TestReactor>(TestReactor)
	}
}