package reactor.dispatch

import reactor.Fn
import reactor.core.Context
import reactor.core.CachingRegistry
import reactor.fn.Consumer
import reactor.fn.Event
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.GroovyTestUtils.$
import static reactor.GroovyTestUtils.consumer

/**
 * @author Jon Brisbin
 */
class DispatcherSpec extends Specification {

	def "Dispatcher executes tasks in correct thread"() {

		given:
		def sameThread = Context.synchronousDispatcher()
		def diffThread = Context.workerPoolDispatcher()
		def currentThread = Thread.currentThread()
		Thread taskThread = null
		def registry = new CachingRegistry<Consumer<Event>>()
		def sel = $('test')
		registry.register(sel, consumer {
			taskThread = Thread.currentThread()
		})

		when: "a task is submitted"
		def t = sameThread.nextTask()
		t.selector = sel
		t.event = Fn.event("Hello World!")
		t.consumerRegistry = registry
		t.submit()

		then: "the task thread should be the current thread"
		currentThread == taskThread

		when: "a task is submitted to the thread pool dispatcher"
		def latch = new CountDownLatch(1)
		t = diffThread.nextTask()
		t.selector = sel
		t.event = Fn.event("Hello World!")
		t.consumerRegistry = registry
		t.setCompletionConsumer({ Event<String> ev -> latch.countDown() } as Consumer<Event<String>>)
		t.submit()

		latch.await(5, TimeUnit.SECONDS) // Wait for task to execute

		then: "the task thread should be different from the current thread"
		taskThread != currentThread

	}

}
