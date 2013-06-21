package reactor.core

import java.lang.management.ManagementFactory
import java.lang.management.ThreadMXBean

import spock.lang.Specification

class EnvironmentSpec extends Specification {

	def "An environment cleans up its Dispatchers when it's shut down"() {
		given: "An Environment"
		ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean()
		int threadCount = threadMxBean.threadCount

		Environment environment = new Environment()

		when: "it is shut down"
		environment.shutdown()

		then: "its dispatchers are cleaned up"
		threadCount == threadMxBean.threadCount
	}
}
