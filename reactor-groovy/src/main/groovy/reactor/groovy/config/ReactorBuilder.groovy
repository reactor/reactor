package reactor.groovy.config

import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.convert.Converter
import reactor.core.Environment
import reactor.core.Reactor
import reactor.core.spec.Reactors
import reactor.event.dispatch.Dispatcher
import reactor.event.registry.Registration
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

	static private Selector noSelector = Selectors.anonymous().t1

	ReactorBuilder linked

	Environment env
	Converter converter
	def eventRoutingStrategy
	Dispatcher dispatcher
	boolean linkParent = true

	private final Map<Selector, List<Consumer>> consumers = [:]
	private final String name
	private final Map<String, ReactorBuilder> reactorMap
	private final List<ReactorBuilder> childNodes = []

	private Reactor reactor

	ReactorBuilder(String name, Map<String, ReactorBuilder> reactorMap) {
		this.reactorMap = reactorMap
		this.name = name
	}

	ReactorBuilder(String name, Map<String, ReactorBuilder> reactorMap, Reactor reactor) {
		this(name, reactorMap)
		this.reactor = reactor
	}

	void init() {
		if (name) {
			if (reactorMap.containsKey(name)) {
				copyConsumersFrom(reactorMap[name])
			}
			reactorMap[name] = this
		}
	}

	Dispatcher dispatcher(String dispatcher) {
		this.dispatcher = env?.getDispatcher(dispatcher)
	}

	ReactorBuilder on(@DelegatesTo(strategy = Closure.DELEGATE_FIRST,
			value = ClosureEventConsumer.ReplyDecorator) Closure closure) {
		on noSelector, new ClosureEventConsumer((Closure) closure.clone())
	}

	ReactorBuilder on(String selector, @DelegatesTo(strategy = Closure.DELEGATE_FIRST,
			value = ClosureEventConsumer.ReplyDecorator) Closure closure) {
		on Selectors.object(selector), new ClosureEventConsumer((Closure) closure.clone())
	}

	ReactorBuilder on(Consumer consumer) {
		on noSelector, consumer
	}

	ReactorBuilder on(String selector, Consumer closure) {
		on Selectors.object(selector), closure
	}

	ReactorBuilder on(Selector selector, @DelegatesTo(strategy = Closure.DELEGATE_FIRST,
			value = ClosureEventConsumer.ReplyDecorator) Closure closure) {
		on selector, new ClosureEventConsumer((Closure) closure.clone())
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
			throw new Exception("not yet implemented")
		}
		if (linked) {
			spec.link(linked.get())
		}

		reactor = spec.get()

		if (childNodes) {
			for (childNode in childNodes) {
				if (childNode.@linkParent && !childNode.@linked) {
					childNode.linked = this
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

		reactor
	}

	void copyConsumersFrom(ReactorBuilder from) {
		Map<Selector, List<Consumer>> fromConsumers = from.consumers
		Map.Entry<Selector, List<Consumer>> consumerEntry

		for (_consumerEntry in fromConsumers) {
			consumerEntry = (Map.Entry<Selector, List<Consumer>>) _consumerEntry
			for (consumer in consumerEntry.value) {
				on consumerEntry.key, (Consumer) consumer
			}
		}
	}

/**
 * initialize a Reactor
 * @param c DSL
 */
	ReactorBuilder reactor(
			@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = NestedReactorBuilder)
			Closure c
	) {
		reactor(null, c)
	}

	ReactorBuilder reactor(String reactorName,
	                       @DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = NestedReactorBuilder)
	                       Closure c
	) {
		def builder = new NestedReactorBuilder(reactorName, this, reactor)
		builder.init()

		DSLUtils.delegateFirstAndRun builder, c

		childNodes << builder
		builder
	}

	final class NestedReactorBuilder extends ReactorBuilder {

		NestedReactorBuilder(String reactorName, ReactorBuilder parent, Reactor reactor) {
			super(reactorName, parent.reactorMap, reactor)
			env = parent.env
			converter = parent.converter
			dispatcher = parent.dispatcher
			consumers.putAll(parent.consumers)
			eventRoutingStrategy = parent.eventRoutingStrategy
		}
	}

}
