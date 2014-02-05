package reactor.timer

import reactor.function.Consumer
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Oleksandr Petrov
 */
class HashWheelTimerBusySpinStrategy extends Specification {

  def period = 50

  def "HashWheelTimer can schedule recurring tasks"() {

    given:
    "a new timer"
    def timer = new HashWheelTimer(10, 8, new HashWheelTimer.BusySpinWait())
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
    timer.cancel()

  }

  def "HashWheelTimer can delay submitted tasks"() {

    given:
    "a new timer"
    def delay = 500
    def timer = new HashWheelTimer(10, 512, new HashWheelTimer.BusySpinWait())
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
    timer.cancel()
  }

}

