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

		given: 'a Processor for events'
		def latch = new CountDownLatch(10)
		List<Data> data = []
		def processor = new reactor.core.processor.spec.ProcessorSpec<Data>().
				dataSupplier({ new Data() } as Supplier<Data>).
				consume({ Data d -> data << d; latch.countDown() } as Consumer<Data>).
				get()

		when: 'a series of events are triggered'
		(1..10).each {
			def op = processor.prepare()
			op.get().type = "test"
			op.commit()
		}

		then: 'the Consumers were run'
		latch.await(1, TimeUnit.SECONDS)
		data.size() == 10

		cleanup:
		processor.shutdown()

	}

	def "Processor provides for multiple event handlers"() {

		given: 'a Processor for events'
		def latch = new CountDownLatch(20)
		List<Data> data = []
		def processor = new reactor.core.processor.spec.ProcessorSpec<Data>().
				dataSupplier({ new Data() } as Supplier<Data>).
				consume({ Data d ->
					data << d
					latch.countDown()
				} as Consumer<Data>).
				consume({ Data d ->
					data << d
					latch.countDown()
				} as Consumer<Data>).
				get()

		when: 'a series of events are triggered'
		processor.batch(10, { Data d ->
			d.type = "test"
		} as Consumer<Data>)

		then: 'the Consumers were run'
		latch.await(1, TimeUnit.SECONDS)
		data.size() == 20

		cleanup:
		processor.shutdown()

	}

}

class Data {
	String type
	String data
}
