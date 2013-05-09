package reactor.spring.context

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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

  }

}

class ReactorAwareBean {
  @Autowired Reactor reactor
}

@Configuration
class ReactorInjectionTestConfig {

  @Bean ReactorFactoryBean reactors() {
    return new ReactorFactoryBean()
  }

  @Bean ReactorAwareBean reactorAwareBean() {
    return new ReactorAwareBean()
  }

}