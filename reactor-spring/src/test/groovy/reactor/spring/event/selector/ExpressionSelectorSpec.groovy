package reactor.spring.event.selector

import reactor.core.Environment
import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.function.Consumer
import spock.lang.Specification

import static reactor.spring.event.selector.ExpressionSelector.E
/**
 * @author Jon Brisbin
 */
class ExpressionSelectorSpec extends Specification {

	Environment env

	def startup() {
		env = new Environment()
	}

	def "SpEL Expressions can be used as Selectors"() {

		given:
			"a plain Reactor"
			def r = Reactors.reactor().get()
			def names = []

		when:
			"a SpEL expression is used as a Selector and the Reactor is notified"
			r.on(E("name == 'John Doe'"), { ev -> names << ev.key.name } as Consumer<Event<TestBean>>)
			r.notify(new TestBean(name: "Jane Doe"))
			r.notify(new TestBean(name: "Jim Doe"))
			r.notify(new TestBean(name: "John Doe"))

		then:
			"only one should have matched"
			names == ["John Doe"]

	}

}

class TestBean {
	String name
}
