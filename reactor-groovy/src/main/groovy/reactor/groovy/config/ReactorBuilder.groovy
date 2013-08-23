package reactor.groovy.config

import groovy.transform.CompileStatic
import reactor.convert.Converter
import reactor.core.Environment
import reactor.core.Reactor
import reactor.core.spec.Reactors
import reactor.event.dispatch.Dispatcher
import reactor.event.selector.Selector
import reactor.event.selector.Selectors
import reactor.function.Consumer
import reactor.function.Supplier
import reactor.groovy.support.ClosureEventConsumer

/**
 * @author Stephane Maldini
 */
@CompileStatic
class ReactorBuilder implements Supplier<Reactor> {

	private static Selector noSelector = Selectors.anonymous().t1

	String name
	Reactor linked
	Environment env
	Converter converter
	def eventRoutingStrategy
	Dispatcher dispatcher

	boolean linkParent = true
	private Map<Selector, List<Consumer>> consumers = [:]
	private Reactor reactor
	private List<ReactorBuilder> childNodes = []

	private Map<String, Reactor> reactorMap

	ReactorBuilder(Map<String, Reactor> reactorMap) {
		this.reactorMap = reactorMap
	}

	void setDispatcher(String dispatcher) {
		this.dispatcher = env.getDispatcher dispatcher
	}

	void setDispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher
	}

	ReactorBuilder on( @DelegatesTo(strategy = Closure.DELEGATE_FIRST,
			value = ClosureEventConsumer.ReplyDecorator) Closure closure) {
		on noSelector, new ClosureEventConsumer((Closure) closure.clone())
	}

	ReactorBuilder on(String selector, @DelegatesTo(strategy = Closure.DELEGATE_FIRST,
			value = ClosureEventConsumer.ReplyDecorator) Closure closure) {
		on Selectors.$(selector), new ClosureEventConsumer((Closure) closure.clone())
	}

	ReactorBuilder on(Consumer consumer) {
		on noSelector, consumer
	}

	ReactorBuilder on(String selector, Consumer closure) {
		on Selectors.$(selector), closure
	}

	ReactorBuilder on(Selector selector,  @DelegatesTo(strategy = Closure.DELEGATE_FIRST,
			value = ClosureEventConsumer.ReplyDecorator) Closure closure) {
		on selector, new ClosureEventConsumer((Closure)closure.clone())
	}

	ReactorBuilder on(Selector selector, Consumer closure) {
		consumers[selector] = consumers[selector] ?: (List<Consumer>) []
		consumers[selector] << closure
		this
	}

	@Override
	Reactor get() {
		if (reactor)
			return reactor

		def spec = Reactors.reactor().env(env).dispatcher(dispatcher)
		if (converter) {
			spec.converters(converter)
		}
		if (eventRoutingStrategy) {

		}
		if (linked) {
			spec.link(linked)
		}

		reactor = spec.get()

		if (childNodes) {
			for (childNode in childNodes) {
				if (childNode.@linkParent && !childNode.@linked) {
					childNode.linked = reactor
				}
				childNode.get()
			}
		}

		if (consumers) {
			for (perSelectorConsumers in consumers.entrySet()) {
				for (consumer in perSelectorConsumers.value) {
					reactor.on((Selector) perSelectorConsumers.key, consumer)
				}
			}
		}

		if (name)
			reactorMap[name] = reactor

		reactor
	}

/**
 * initialize a Reactor
 * @param c DSL
 */
	ReactorBuilder reactor(
			@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = NestedReactorBuilder) Closure c
	) {
		def builder = new NestedReactorBuilder(this)
		DSLUtils.delegateFirstAndRun builder, c

		childNodes << builder
		builder
	}

	final class NestedReactorBuilder extends ReactorBuilder {

		NestedReactorBuilder(ReactorBuilder parent) {
			super(parent.reactorMap)
			env = parent.env
			converter = parent.converter
			dispatcher = parent.dispatcher
			consumers = parent.consumers
			eventRoutingStrategy = parent.eventRoutingStrategy
		}
	}

}
