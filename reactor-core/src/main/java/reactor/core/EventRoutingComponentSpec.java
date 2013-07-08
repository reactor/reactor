package reactor.core;

import reactor.convert.Converter;
import reactor.convert.DelegatingConverter;
import reactor.event.registry.SelectionStrategy;
import reactor.event.registry.TagAwareSelectionStrategy;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringEventRouter;
import reactor.event.routing.ConsumerInvoker;
import reactor.event.routing.EventRouter;
import reactor.filter.*;

import java.util.List;

/**
 * @author Jon Brisbin
 */
public abstract class EventRoutingComponentSpec<SPEC extends EventRoutingComponentSpec<SPEC, TARGET>, TARGET> extends DispatcherComponentSpec<SPEC, TARGET> {

	protected Converter            converter;
	protected EventRoutingStrategy eventRoutingStrategy;
	protected SelectionStrategy    selectionStrategy;

	public SPEC selectionStrategy(SelectionStrategy selectionStrategy) {
		this.selectionStrategy = selectionStrategy;
		return (SPEC) this;
	}

	public SPEC converters(Converter... converters) {
		this.converter = new DelegatingConverter(converters);
		return (SPEC) this;
	}

	public SPEC converters(List<Converter> converters) {
		this.converter = new DelegatingConverter(converters);
		return (SPEC) this;
	}

	public SPEC broadcastEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.BROADCAST;
		return (SPEC) this;
	}

	public SPEC randomEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.RANDOM;
		return (SPEC) this;
	}

	public SPEC firstEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.FIRST;
		return (SPEC) this;
	}

	public SPEC roundRobinEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.ROUND_ROBIN;
		return (SPEC) this;
	}

	public SPEC tagFiltering() {
		this.selectionStrategy = new TagAwareSelectionStrategy();
		return (SPEC) this;
	}

	protected Reactor createReactor() {
		final Reactor reactor;
		if (null == this.dispatcher && env != null) {
			this.dispatcher = env.getDefaultDispatcher();
		}
		if (null == this.reactor) {
			reactor = new Reactor(env,
														dispatcher,
														selectionStrategy,
														createEventRouter());
		} else {
			reactor = new Reactor(
					env,
					null == dispatcher ? this.reactor.getDispatcher() : dispatcher,
					null == selectionStrategy ? this.reactor.getConsumerRegistry().getSelectionStrategy() : selectionStrategy,
					createEventRouter(this.reactor));
		}
		return reactor;
	}

	private EventRouter createEventRouter(Reactor reactor) {
		if (converter == null && eventRoutingStrategy == null) {
			return reactor.getEventRouter();
		} else {
			ConsumerInvoker consumerInvoker;
			if (converter == null) {
				consumerInvoker = ((ConsumerFilteringEventRouter) reactor.getEventRouter()).getConsumerInvoker();
			} else {
				consumerInvoker = new ArgumentConvertingConsumerInvoker(converter);
			}
			Filter filter = getFilter(((ConsumerFilteringEventRouter) reactor.getEventRouter()).getFilter());
			return new ConsumerFilteringEventRouter(filter, consumerInvoker);
		}
	}

	private EventRouter createEventRouter() {
		return new ConsumerFilteringEventRouter(getFilter(null), new ArgumentConvertingConsumerInvoker(converter));
	}

	private Filter getFilter(Filter existingFilter) {
		Filter filter;
		if (EventRoutingStrategy.ROUND_ROBIN == eventRoutingStrategy) {
			filter = new RoundRobinFilter();
		} else if (EventRoutingStrategy.RANDOM == eventRoutingStrategy) {
			filter = new RandomFilter();
		} else if (EventRoutingStrategy.FIRST == eventRoutingStrategy) {
			filter = new FirstFilter();
		} else {
			if (null == existingFilter) {
				filter = new PassThroughFilter();
			} else {
				filter = existingFilter;
			}
		}
		return filter;
	}

	protected enum EventRoutingStrategy {
		BROADCAST, RANDOM, ROUND_ROBIN, FIRST
	}

}
