package reactor.groovy.config

import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.reactivestreams.Processor
import reactor.Environment
import reactor.bus.Event
import reactor.bus.EventBus
import reactor.bus.convert.Converter
import reactor.bus.filter.*
import reactor.bus.registry.CachingRegistry
import reactor.bus.routing.ArgumentConvertingConsumerInvoker
import reactor.bus.routing.ConsumerInvoker
import reactor.bus.routing.Router
import reactor.bus.selector.Selector
import reactor.bus.selector.Selectors
import reactor.core.Dispatcher
import reactor.core.dispatch.SynchronousDispatcher
import reactor.fn.Consumer
import reactor.fn.Supplier
import reactor.groovy.support.ClosureEventConsumer
import reactor.rx.Stream
import reactor.rx.Streams
import reactor.rx.action.Action

/**
 * @author Stephane Maldini
 */
@CompileStatic
class ReactorBuilder implements Supplier<EventBus> {

	static final private Selector noSelector = Selectors.anonymous()
	static final private Filter DEFAULT_FILTER = new PassThroughFilter()

	static final String ROUND_ROBIN = 'round-robin'
	static final String PUB_SUB = 'all'
	static final String RANDOM = 'random'
	static final String FIRST = 'first'

	Environment env
	Converter converter
	Router router
	ConsumerInvoker consumerInvoker
	Dispatcher dispatcher
	Filter filter
	boolean override = false

	private String dispatcherName

	final String name

	private final SortedSet<SelectorProcessor> processors = new TreeSet<SelectorProcessor>()
	private final Map<String, Object> ext = [:]
	private final Map<Selector, List<Consumer>> consumers = [:]
	private final Map<String, ReactorBuilder> reactorMap
	private final List<ReactorBuilder> childNodes = []
	private EventBus reactor

	ReactorBuilder(String name, Map<String, ReactorBuilder> reactorMap) {
		this.reactorMap = reactorMap
		this.name = name
	}

	ReactorBuilder(String name, Map<String, ReactorBuilder> reactorMap, EventBus reactor) {
		this(name, reactorMap)
		this.reactor = reactor
	}

	void rehydrate(ReactorBuilder r) {
		converter = converter ?: r.converter
		filter = filter ?: r.filter
		dispatcher = dispatcher ?: r.dispatcher
		dispatcherName = dispatcherName ?: r.dispatcherName
		router = router ?: r.router
		consumerInvoker = consumerInvoker ?: r.consumerInvoker

		if (!override) {
			processors.addAll r.processors
			childNodes.addAll r.childNodes
		}

		for (entry in r.ext) {
			if (!ext[((Map.Entry<String, Object>) entry).key]) ext[((Map.Entry<String, Object>) entry).key] =
					((Map.Entry<String, Object>) entry).value
		}
	}

	void init() {
		if (name) {
			def r = reactorMap[name]
			if (r) {
				rehydrate r
				addConsumersFrom r
			}
			reactorMap[name] = this
		}
	}

	def ext(String k) {
		ext[k]
	}

	void ext(String k, v) {
		ext[k] = v
	}

	void exts(Map<String, Object> map) {
		ext.putAll map
	}

	Filter routingStrategy(String strategy) {
		switch (strategy) {
			case ROUND_ROBIN:
				filter = new RoundRobinFilter()
				break
			case RANDOM:
				filter = new RandomFilter()
				break
			case FIRST:
				filter = new FirstFilter()
				break
			case PUB_SUB:
			default:
				filter = DEFAULT_FILTER
		}
	}

	ReactorBuilder dispatcher(String dispatcher) {
		this.dispatcherName = dispatcher
		this.dispatcher = env?.getDispatcher(dispatcher)
		this
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

	void stream(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Stream)
	            @ClosureParams(value=SimpleType, options="Event")
	            Closure<Action<Event<?>, Event<?>>> closure) {
		stream((Selector) null, closure)
	}

	void stream(String selector,
	            @DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Stream)
	            @ClosureParams(value=SimpleType, options="Event")
	            Closure<Action<Event<?>, Event<?>>> closure) {
		stream Selectors.$(selector), closure
	}

	void stream(Selector selector,
	            @DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Stream)
	            @ClosureParams(value=SimpleType, options="Event")
	            Closure<Action<Event<?>, Event<?>>> closure) {
		Stream<Event<?>> head = Streams.<Event<?>> broadcast(env, SynchronousDispatcher.INSTANCE)
		Processor<Event<?>, Event<?>> newTail = DSLUtils.delegateFirstAndRun(head, closure)?.combine()
		if(!newTail){
			throw new IllegalArgumentException("A Stream closure must return a non null reactor.rx.Action")
		}
		processor selector, newTail
	}

	void processor(String selector, Processor<?, Event> _processor) {
		processor Selectors.object(selector), _processor
	}

	void processor(Selector selector, Processor<?, Event> _processor) {
		processors.add(new SelectorProcessor((Processor<Event<?>,Event<?>>)_processor, selector ?: Selectors.matchAll()))
	}

	@Override
	EventBus get() {
		if (reactor)
			return reactor

		def spec = EventBus.config().env(env)
		if (dispatcherName) {
			spec.dispatcher(dispatcherName)
		} else {
			spec.dispatcher(dispatcher)
		}
		if (converter) {
			spec.converters(converter)
		}
		if (router) {
			spec.eventRouter(router)
		} else if (processors) {

			def registry = new CachingRegistry<Processor<Event<?>,Event<?>>>()
			Iterator<SelectorProcessor> it = processors.iterator()
			SelectorProcessor p
			while(it.hasNext()){
				p = it.next()
				registry.register p.selector, p.processor
			}

			spec.eventRouter(new StreamRouter(filter ?: DEFAULT_FILTER,
					consumerInvoker ?: new ArgumentConvertingConsumerInvoker(converter), registry))

		} else {
			if (filter) {
				spec.eventFilter(filter)
			}
			if (consumerInvoker) {
				spec.consumerInvoker(consumerInvoker)
			}
		}

		reactor = spec.get()

		if (childNodes) {
			for (childNode in childNodes) {
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

	void addConsumersFrom(ReactorBuilder from) {
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

	@CompileStatic
	final class NestedReactorBuilder extends ReactorBuilder {

		NestedReactorBuilder(String reactorName, ReactorBuilder parent, EventBus reactor) {
			super(reactorName, parent.reactorMap, reactor)
			rehydrate parent

			env = parent.env
			consumers.putAll(parent.consumers)
		}
	}


	@CompileStatic
	final class SelectorProcessor implements Comparable<SelectorProcessor> {
		final Processor<Event<?>, Event<?>> processor
		final Selector selector

		SelectorProcessor(Processor<Event<?>, Event<?>> processor, Selector selector) {
			this.processor = processor
			this.selector = selector
		}

		@Override
		int compareTo(SelectorProcessor o) {
			selector ? 1 : 0
		}
	}
}
