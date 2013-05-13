package reactor.fn

import reactor.Fn
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
 * @author Stephane Maldini
 */
class SelectorSpec extends Specification {

	def "Selectors work with Strings"() {

		when: "a selector backed by a String is defined"
		def selector = $("test")

		then: "it matches the string"
		selector.matches "test"
	}

	def "Selectors work with primitives"() {

		when: "a selector backed by a primitve is defined"
		def selector = $(1L)

		then: "it matches the primitive"
		selector.matches 1L
	}

	def "Class selectors match on isAssignableFrom"() {

		when: "a selector based on Class is defined"
		def clz1 = T(Throwable)
		def clz2 = T(IllegalArgumentException)

		then: "it matches the class and its sub types"
		clz1.matches Throwable
		clz1.matches IllegalArgumentException
	}

	def "Regex selectors match on expressions"() {

		when: "A selector based on a regular expression and a matching key are defined"
		def sel1 = Fn.R("test([0-9]+)")
		def key = "test1"

		then: "they match"
		sel1.matches "test1"

		when: "a key the does not fit the pattern is provided"
		def key2 = "test-should-not-match"

		then: "it does not match"
		!sel1.matches(key2)

	}

	def "Selectors can be filtered by tag"() {

		given: "A tag-aware SelectionStrategy"
		def sel1 = $("test1").setTags("one", "two")
		def sel2 = $("test2").setTags("three", "four")
		def sel3 = $("test2").setTags("one", "three")
		def strategy = new TagAwareSelectionStrategy()

		when:
		def match1 = strategy.matches(sel1, new TaggableKey('test2').setTags('three', 'four'))
		def match2 = strategy.matches(sel1, new TaggableKey('test2').setTags('one', 'three'))
		def match3 = strategy.matches(sel2, new TaggableKey('test2').setTags('one', 'three'))

		then:
		!match1
		!match2
		match3
	}

	def "Selectors can be matched on URI"() {

		given: "A UriTemplateSelector"
		def sel1 = U("/path/to/{resource}")
		def key = "/path/to/resourceId"
		def r = R.create(true)
		def resourceId = ""
		r.on(sel1, consumer { Event<String> ev ->
			resourceId = ev.data
		})

		when: "The selector is matched"
		r.notify key, Fn.event("resourceId")

		then: "The resourceId has been set"
		resourceId

	}

	def "SelectionStrategy can be load balanced"() {

		given: "a set of consumers assigned to the same selector"
		def r = R.create(true)
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

		then: "random selection of consumers have been called"
		assertThat(called, anyOf(hasItem(1), hasItem(2), hasItem(3), hasItem(4)))

	}
}
