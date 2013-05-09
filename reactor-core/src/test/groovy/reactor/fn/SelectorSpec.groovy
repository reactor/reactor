package reactor.fn

import reactor.Fn
import reactor.core.Context
import reactor.core.R
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static org.hamcrest.CoreMatchers.*
import static org.hamcrest.MatcherAssert.assertThat
import static reactor.Fn.T
import static reactor.Fn.U
import static reactor.GroovyTestUtils.$
import static reactor.GroovyTestUtils.consumer
import static reactor.fn.Registry.LoadBalancingStrategy.RANDOM
import static reactor.fn.Registry.LoadBalancingStrategy.ROUND_ROBIN

/**
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
class SelectorSpec extends Specification {

	def "Selectors work with primitive types"() {

		when: "selectors with the same string are defined"
		def str1 = $("test")
		def str2 = $("test")
		def l1 = $(1l)
		def l2 = $(1l)

		then: "they match"
		str1.matches str2
		l1.matches l2

	}

	def "Class selectors match on isAssignableFrom"() {

		when: "selectors based on Class are defined"
		def clz1 = T(Throwable)
		def clz2 = T(IllegalArgumentException)

		then: "they match"
		clz1.matches clz2

	}

	def "Regex selectors match on expressions"() {

		when: "selectors based on a regular expression are defined"
		def sel1 = Fn.R("test([0-9]+)")
		def sel2 = $("test1")

		then: "they match"
		sel1.matches sel2

		when: "selectors that don't fit the pattern are defined"
		def sel3 = $("test-should-not-match")

		then: "they don't match"
		!sel1.matches(sel3)

	}

	def "Selectors can be filtered by tag"() {

		given: "A tag-aware SelectionStrategy"
		def sel1 = $("test1").setTags("one", "two")
		def sel2 = $("test2").setTags("three", "four")
		def sel3 = $("test2").setTags("one", "three")
		def strategy = new TagAwareSelectionStrategy()

		when:
		def match1 = strategy.matches(sel1, sel2)
		def match2 = strategy.matches(sel1, sel3)
		def match3 = strategy.matches(sel2, sel3)

		then:
		!match1
		!match2
		match3

	}

	def "Selectors can be matched on URI"() {

		given: "A UriTemplateSelector"
		def sel1 = U("/path/to/{resource}")
		def sel2 = $("/path/to/resourceId")
		def r = R.create().setDispatcher(Context.synchronousDispatcher())
		def resourceId = ""
		r.on(sel1, consumer { Event<String> ev ->
			resourceId = ev.data
		})

		when: "The selectors are matched"
		r.notify sel2, Fn.event("resourceId")

		then: "The resourceId has been set"
		resourceId

	}


	def "SelectionStrategy can be load balanced"() {

		given: "a set of consumers assigned to the same selector"
		def r = R.create().setDispatcher(Context.synchronousDispatcher())
		def latch = new CountDownLatch(4)
		def called = []
		def a1 = {
			called << 1
			latch.countDown()
		}
		def a2 = {
			called << 2
			latch.countDown()
		}
		def a3 = {
			called << 3
			latch.countDown()
		}
		def a4 = {
			called << 4
			latch.countDown()
		}
		r.on(Fn.compose(a1))
		r.on(Fn.compose(a2))
		r.on(Fn.compose(a3))
		r.on(Fn.compose(a4))

		when: "events are triggered with ROUND_ROBIN"
		r.consumerRegistry.loadBalancingStrategy = ROUND_ROBIN
		(1..4).each {
			r.notify(Fn.event("Hello World!"))
		}

		then: "all consumers should have been called once"
		assertThat(called, hasItems(1, 2, 3, 4))

		when: "events are triggered with RANDOM"
		called.clear()
		r.consumerRegistry.loadBalancingStrategy = RANDOM
		(1..4).each {
			r.notify(Fn.event("Hello World!"))
		}

		then: "random selection of consumer shave been called"
		assertThat(called, anyOf(hasItem(1), hasItem(2), hasItem(3), hasItem(4)))

	}

}
