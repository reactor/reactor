package reactor.spring.config

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import reactor.core.Environment
import reactor.core.Reactor
import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.spring.annotation.ReplyTo
import reactor.spring.annotation.Selector
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class ReactorBeanDefinitionRegistrarSpec extends Specification {

  def "EnableReactor annotation causes default components to be created"() {

    given:
      "an annotated configuration"
      def appCtx = new AnnotationConfigApplicationContext(ReactorConfig)
      def consumer = appCtx.getBean(LoggingConsumer)
      def reactor = appCtx.getBean(Reactor)

    when:
      "notifying the injected Reactor"
      reactor.notify("test", Event.wrap("World"))

    then:
      "the method will have been invoked"
      consumer.count == 2

    when:
      "notifying the injected Reactor with replyTo header"
      reactor.notify("test2", Event.wrap("World").setReplyTo("reply2"))

    then:
      "the method will have been invoked"
      consumer.count == 4

  }

  def "ReplyTo annotation causes replies to be handled"() {

    given:
      'an annotated configuration'
      def appCtx = new AnnotationConfigApplicationContext(ReactorConfig)
      def consumer = appCtx.getBean(LoggingConsumer)
      def reactor = appCtx.getBean(Reactor)

    when:
      "notifying the injected Reactor"
      reactor.notify("test", Event.wrap("World"))

    then:
      "the method will have been invoked"
      consumer.count == 2

  }

}

@Component
class LoggingConsumer {
  @Autowired Reactor reactor
  long count = 0
  Logger log = LoggerFactory.getLogger(LoggingConsumer)


	@Selector("test2")
	@ReplyTo
	String onTest2(String s) {
		log.info("Hello again ${s}!")
		"Goodbye again ${s}!"
		count++
	}

	@Selector("reply2")
	void onReply2(String s) {
		log.info("Got reply: ${s}")
		count++
	}

  @Selector("test")
  @ReplyTo("reply")
  String onTest(String s) {
    log.info("Hello ${s}!")
    count++
    "Goodbye ${s}!"
  }

  @Selector("reply")
  void onReply(String s) {
    log.info("Got reply: ${s}")
    count++
  }
}

@Configuration
@EnableReactor("default")
@ComponentScan
class ReactorConfig {

  @Bean Reactor reactor(Environment env) {
    Reactors.reactor().env(env).synchronousDispatcher().get()
  }

}
