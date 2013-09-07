package reactor.groovy.config

import groovy.transform.CompileStatic
import reactor.convert.Converter
import reactor.core.Environment
import reactor.core.Reactor
import reactor.core.composable.Deferred
import reactor.core.composable.Stream
import reactor.core.composable.spec.Streams
import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.event.dispatch.Dispatcher
import reactor.event.routing.ArgumentConvertingConsumerInvoker
import reactor.event.routing.ConsumerInvoker
import reactor.event.routing.EventRouter
import reactor.event.selector.Selector
import reactor.event.selector.Selectors
import reactor.event.support.CallbackEvent
import reactor.filter.Filter
import reactor.filter.FirstFilter
import reactor.filter.PassThroughFilter
import reactor.filter.RandomFilter
import reactor.filter.RoundRobinFilter
import reactor.function.Consumer
import reactor.function.Predicate
import reactor.function.Supplier
import reactor.groovy.support.ClosureEventConsumer

/**
 * @author Stephane Maldini
 */
@CompileStatic
class ReactorBuilder implements Supplier<Reactor> {

	static final private Selector noSelector = Selectors.anonymous().t1
	static final private Filter DEFAULT_FILTER = new PassThroughFilter()

	static final String ROUND_ROBIN = 'round-robin'
	static final String PUB_SUB = 'all'
	static final String RANDOM = 'random'
	static final String FIRST = 'first'

	ReactorBuilder linked
	Environment env
	Converter converter
	EventRouter router
	ConsumerInvoker consumerInvoker
	Dispatcher dispatcher
	Filter filter
	boolean linkParent = true
	boolean override = false

	private String dispatcherName

	final String name

	private final SortedSet<HeadAndTail> streams = new TreeSet<HeadAndTail>()
	private final Map<String, Object> ext = [:]
	private final Map<Selector, List<Consumer>> consumers = [:]
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

	void rehydrate(ReactorBuilder r) {
		converter = converter ?: r.converter
		filter = filter ?: r.filter
		dispatcher = dispatcher ?: r.dispatcher
		dispatcherName = dispatcherName ?: r.dispatcherName
		router = router ?: r.router
		consumerInvoker = consumerInvoker ?: r.consumerInvoker

		if(!override){
			streams.addAll r.streams
			childNodes.addAll r.childNodes
		}

		for(entry in r.ext){
			if(!ext[((Map.Entry<String, Object>)entry).key]) ext[((Map.Entry<String, Object>)entry).key] =
					((Map.Entry<String, Object>)entry).value
		}
	}

	void init() {
		if (name) {
			def r = reactorMap[name]
			if (r) {
				rehydrate r
				addConsumersFrom r
				linked = r.linked ? reactorMap[r.linked.name] : null
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

	void stream(@DelegatesTo(strategy = Closure.DELEGATE_FIRST, value = Stream) Closure<Stream> closure) {
		stream((Selector)null, closure)
	}

	void stream(Deferred<Event<?>, Stream<Event<?>>> head, Stream<Event<?>> tail) {
		stream((Selector)null, head, tail)
	}

	void stream(String selector, @DelegatesTo(strategy = Closure.DELEGATE_FIRST,
			value = Stream) Closure<Stream> closure) {
		stream Selectors.$(selector), closure
	}

	void stream(Selector selector, @DelegatesTo(strategy = Closure.DELEGATE_FIRST,
			value = Stream) Closure<Stream> closure) {
		Deferred<Event<?>, Stream<Event<?>>> head = Streams.<Event<?>> defer().get()
		Stream newTail = DSLUtils.delegateFirstAndRun(head.compose(), closure)
		stream selector, head, newTail
	}


	void stream(Selector selector, Deferred<Event<?>, Stream<Event<?>>> head, Stream<Event<?>> tail) {
		streams << new HeadAndTail(head, tail, selector)
	}

	@Override
	Reactor get() {
		if (reactor)
			return reactor

		def spec = Reactors.reactor().env(env)
		if(dispatcherName){
			spec.dispatcher(dispatcherName)
		}else{
			spec.dispatcher(dispatcher)
		}
		if (converter) {
			spec.converters(converter)
		}
		if (router) {
			spec.eventRouter(router)
		} else if (streams) {
			Deferred<Event<?>, Stream<Event<?>>> deferred = null
			Stream<Event<?>> tail = null
			HeadAndTail stream
			HeadAndTail anticipatedStream
			Iterator<HeadAndTail> it = streams.iterator()
			boolean first = true

			while (it.hasNext() || anticipatedStream) {
				stream = anticipatedStream ?: it.next()
				anticipatedStream = null

				if (first) {
					first = false
					deferred = Streams.<Event<?>>defer().get()
					tail = deferred.compose()
				}
				if (stream.selector) {
					if(it.hasNext()){
						anticipatedStream = it.next()
					}else{
						def finalDeferred = Streams.<Event<?>>defer().get()
						anticipatedStream = new HeadAndTail(finalDeferred, finalDeferred.compose(), null)
					}
					tail.filter(new EventRouterPredicate(stream.selector), anticipatedStream.tail).consume(stream.head.compose())
				} else {
					tail.consume(stream.head.compose())
				}
				tail = stream.tail
			}

			tail.consumeEvent(new Consumer<Event>() {
				@Override
				void accept(Event eventEvent) {
					if(eventEvent.class == CallbackEvent)
						((CallbackEvent)eventEvent).callback()
				}
			})
			spec.eventRouter(new StreamEventRouter(filter ?: DEFAULT_FILTER,
					consumerInvoker ?: new ArgumentConvertingConsumerInvoker(converter), deferred))

		} else {
			if (filter) {
				spec.eventFilter(filter)
			}
			if (consumerInvoker) {
				spec.consumerInvoker(consumerInvoker)
			}
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
	private final class EventRouterPredicate extends Predicate<Event<?>> {
		final Selector sel

		EventRouterPredicate(Selector sel) {
			this.sel = sel
		}

		@Override
		boolean test(Event<?> event) {
			sel.matches(event.headers.get(StreamEventRouter.KEY_HEADER))
		}
	}

	@CompileStatic
	final class HeadAndTail implements Comparable<HeadAndTail> {
		final Deferred<Event<?>, Stream<Event<?>>> head
		final Stream<Event<?>> tail
		final Selector selector

		HeadAndTail(Deferred<Event<?>, Stream<Event<?>>> head, Stream<Event<?>> tail, Selector selector) {
			this.head = head
			this.tail = tail
			this.selector = selector
		}

		@Override
		int compareTo(HeadAndTail o) {
			selector ? 1 : 0
		}
	}

	@CompileStatic
	final class NestedReactorBuilder extends ReactorBuilder {

		NestedReactorBuilder(String reactorName, ReactorBuilder parent, Reactor reactor) {
			super(reactorName, parent.reactorMap, reactor)
			rehydrate parent

			env = parent.env
			linked = parent.linked
			consumers.putAll(parent.consumers)
		}
	}

}
