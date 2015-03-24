package reactor.bus

import reactor.bus.batcher.spec.OperationBatcherSpec
import reactor.fn.Consumer
import reactor.fn.Supplier
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 */
class OperationBatchersSpec extends Specification {

	def "OperationBatcher provides high-speed event processor"() {

		given: 'an OperationBatcher for events'
		def latch = new CountDownLatch(10)
		List<Data> data = []
		def processor = new OperationBatcherSpec<Data>().
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
		def latch = new CountDownLatch(3)
		def data = []
		def consumer = { Data d -> data << d.data; latch.countDown() } as Consumer<Data>
		def processor = new OperationBatcherSpec<Data>().
				dataSupplier({ new Data() } as Supplier<Data>).
				consume(consumer).
				consume(consumer).
				consume(consumer).
				get()

		when: 'a series of events are triggered'
		def i = 0
		processor.batch(3, { Data d ->
			d.type = "test"
			d.data = "run ${i++}"
		} as Consumer<Data>)

		then: 'the Consumers were run'
		latch.await(1, TimeUnit.SECONDS)
		data.size() == 3

		cleanup:
		processor.shutdown()

	}

	def "Processor can handle batches larger than the backlog"() {

		given: 'a Processor for events'
		def latch = new CountDownLatch(300)
		def consumer = { Data d -> latch.countDown() } as Consumer<Data>
		def processor = new OperationBatcherSpec<Data>().
				dataBufferSize(128).
				dataSupplier({ new Data() } as Supplier<Data>).
				consume(consumer).
				get()

		when: 'a series of events are triggered'
		def i = 0
		processor.batch(300, { Data d ->
			d.type = "test"
			d.data = "run ${i++}"
		} as Consumer<Data>)

		then: 'the Consumers were run'
		latch.await(1, TimeUnit.SECONDS)

		cleanup:
		processor.shutdown()

	}

}

class Data {
	String type
	String data
}
