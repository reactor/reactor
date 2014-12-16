package reactor.core.dispatch.forkjoin

import reactor.Environment
import reactor.fn.Function
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static reactor.GroovyTestUtils.consumer
import static reactor.GroovyTestUtils.function

/**
 * @author Jon Brisbin
 */
class ForkJoinPoolSpec extends Specification {

	Random random
	Environment env
	ForkJoinPool pool
	Function task

	def setup() {
		random = new Random(System.nanoTime())
		env = new Environment()
		pool = new ForkJoinPool(env)
		task = function { v ->
			Thread.sleep(random.nextInt(500))
			return Thread.currentThread()
		}
	}

	def cleanup(){
		env.shutdown()
	}

	def "ForkJoinPool forks tasks"() {

		given: "a standard pool"
			def main = Thread.currentThread()

		when: "tasks are forked"
			def task = pool.join(task, task, task, task)
			def results = task.compose()
			task.submit()

		then: "tasks were run in another thread"
			!results.next().await(5, TimeUnit.SECONDS)?.find { it == main }

	}

	def "ForkJoinPool collects values"() {

		given: "a standard pool"
			def latch = new CountDownLatch(4)

		when: "tasks are forked"
			def fj = pool.fork()
			fj.compose().
					buffer(4).
					consume(consumer { threads -> threads.each { latch.countDown() } })
			(1..4).each {
				fj.add(task).submit()
			}
			fj.submit()

		then: "tasks were run in another thread"
			latch.await(5, TimeUnit.SECONDS)

	}

}
