package reactor.fn.timer

import reactor.core.support.wait.BusySpinWaitStrategy
import reactor.core.timer.HashWheelTimer
import reactor.fn.Consumer
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Oleksandr Petrov
 * @author Stephane Maldini
 */
class HashWheelTimerBusySpinStrategy extends Specification {

	def period = 50

	def "HashWheelTimer can schedule recurring tasks"() {

		given:
			"a new globalTimer"
			def timer = new HashWheelTimer(10, 8, new BusySpinWaitStrategy())
			timer.start()
			def latch = new CountDownLatch(10)

		when:
			"a task is submitted"
			timer.schedule(
					{ Long now -> latch.countDown() } as Consumer<Long>,
					period,
					TimeUnit.MILLISECONDS,
					period
			)

		then:
			"the latch was counted down"
			latch.await(1, TimeUnit.SECONDS)

		when:
			"Cancelled"
			timer.cancel()

		then:
			timer.isCancelled()

	}

	def "HashWheelTimer can delay submitted tasks"() {

		given:
			"a new globalTimer"
			def delay = 500
			def timer = new HashWheelTimer(10, 512, new BusySpinWaitStrategy())
			timer.start()
			def latch = new CountDownLatch(1)
			def start = System.currentTimeMillis()
			def elapsed = 0
			//def actualTimeWithinBounds = true

		when:
			"a task is submitted"
			timer.submit(
					{ Long now ->
						elapsed = System.currentTimeMillis() - start
						latch.countDown()
					} as Consumer<Long>,
					delay,
					TimeUnit.MILLISECONDS
			)

		then:
			"the latch was counted down"
			latch.await(1, TimeUnit.SECONDS)
			elapsed >= delay
			elapsed < delay * 2

		cleanup:
			timer.cancel()
	}

}

