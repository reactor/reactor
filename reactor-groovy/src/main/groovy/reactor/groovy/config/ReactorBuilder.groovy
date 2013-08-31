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
import reactor.filter.Filter
import reactor.filter.FirstFilter
import reactor.filter.PassThroughFilter
import reactor.filter.RandomFilter
import reactor.filter.RoundRobinFilter
import reactor.function.Consumer
import reactor.function.Supplier
import reactor.groovy.support.ClosureEventConsumer

/**
 * @author Stephane Maldini
 */
@CompileStatic
class ReactorBuilder implements Supplier<Reactor> {

	static private Selector noSelector = Selectors.anonymous().t1

	static final String ROUND_ROBIN = 'round-robin'
	static final String PUB_SUB = 'all'
	static final String RANDOM = 'random'
	static final String FIRST = 'first'

	ReactorBuilder linked

	Environment env
	Converter converter
	Filter filter
	Dispatcher dispatcher
	boolean linkParent = true

	private final Map<String, Object> ext = [:]
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
			def r = reactorMap[name]
			if (r) {
				copyConsumersFrom(r)
				converter = r.converter
				filter = r.filter
				dispatcher = r.dispatcher
				linked = r.linked ? reactorMap[r.linked.name] : null
			}
			reactorMap[name] = this
		}
	}

	def ext(String k){
		ext[k]
	}

	void ext(String k, v){
		ext[k] = v
	}

	void exts(Map<String, Object> map){
		ext.putAll map
	}

	Filter routingStrategy(String strategy){
		switch (strategy){
			case ROUND_ROBIN:
				filter = new RoundRobinFilter()
				break
			case RANDOM:
				filter = new RandomFilter()
				break
			case FIRST:
				filter = new FirstFilter()
				break
			default:
				filter = new PassThroughFilter()
				break
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
		if (filter) {
			spec.eventRoutingFilter(filter)
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
			filter = parent.filter
			linked = parent.linked
			consumers.putAll(parent.consumers)
		}
	}

}
