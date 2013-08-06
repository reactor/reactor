package reactor.core.processor

import reactor.function.Consumer
import reactor.function.Supplier
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 */
class ProcessorSpec extends Specification {

  def "Processor provides high-speed event processor"() {

    given:
      'a RingBuffer for Events'
      def latch = new CountDownLatch(10)
      List<Data> data = []
      def processor = new reactor.core.processor.spec.ProcessorSpec<Data>().
          dataSupplier({ new Data() } as Supplier<Data>).
          consume({ Data d -> data << d; latch.countDown() } as Consumer<Data>).
          get()

    when:
      'a series of Events are triggered'
      (1..10).each {
        def op = processor.prepare()
        op.get().type = "test"
        op.commit()
      }

    then:
      'the Consumers were run'
      latch.await(1, TimeUnit.SECONDS)
      data.size() == 10

    cleanup:
      processor.shutdown()

  }

}

class Data {
  String type
  String data
}
