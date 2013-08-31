/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.spec.support;

import java.util.List;

import reactor.convert.Converter;
import reactor.convert.DelegatingConverter;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.event.dispatch.Dispatcher;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringEventRouter;
import reactor.event.routing.EventRouter;
import reactor.filter.Filter;
import reactor.filter.FirstFilter;
import reactor.filter.PassThroughFilter;
import reactor.filter.RandomFilter;
import reactor.filter.RoundRobinFilter;


/**
 * A generic environment-aware class for specifying components that need to be configured
 * with an {@link Environment}, {@link Dispatcher}, and {@link EventRouter}.
 *
 * @param <SPEC>   The DispatcherComponentSpec subclass
 * @param <TARGET> The type that this spec will create
 * @author Jon Brisbin
 */
@SuppressWarnings("unchecked")
public abstract class EventRoutingComponentSpec<SPEC extends EventRoutingComponentSpec<SPEC, TARGET>, TARGET> extends DispatcherComponentSpec<SPEC, TARGET> {

	private Converter            converter;
	private EventRoutingStrategy eventRoutingStrategy;
	private EventRouter          eventRouter;
	private Filter               eventFilter;

	/**
	 * Configures the component's EventRouter to use the given {code converters}.
	 *
	 * @param converters The converters to be used by the event router
	 * @return {@code this}
	 */
	public final SPEC converters(Converter... converters) {
		this.converter = new DelegatingConverter(converters);
		return (SPEC) this;
	}

	/**
	 * Configures the component's EventRouter to use the given {code converters}.
	 *
	 * @param converters The converters to be used by the event router
	 * @return {@code this}
	 */
	public final SPEC converters(List<Converter> converters) {
		this.converter = new DelegatingConverter(converters);
		return (SPEC) this;
	}


	/**
	 * Assigns the component's Filter
	 *
	 * @return {@code this}
	 */
	public final SPEC eventFilter(Filter filter) {
		this.eventFilter = filter;
		return (SPEC) this;
	}

	/**
	 * Assigns the component's EventRouter
	 *
	 * @return {@code this}
	 */
	public final SPEC eventRouter(EventRouter router) {
		this.eventRouter = router;
		return (SPEC) this;
	}

	/**
	 * Configures the component's EventRouter to broadcast events to all matching
	 * consumers
	 *
	 * @return {@code this}
	 */
	public final SPEC broadcastEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.BROADCAST;
		return (SPEC) this;
	}

	/**
	 * Configures the component's EventRouter to route events to one consumer that's
	 * randomly selected from that matching consumers
	 *
	 * @return {@code this}
	 */
	public final SPEC randomEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.RANDOM;
		return (SPEC) this;
	}

	/**
	 * Configures the component's EventRouter to route events to the first of the matching
	 * consumers
	 *
	 * @return {@code this}
	 */
	public final SPEC firstEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.FIRST;
		return (SPEC) this;
	}

	/**
	 * Configures the component's EventRouter to route events to one consumer selected
	 * from the matching consumers using a round-robin algorithm
	 * consumers
	 *
	 * @return {@code this}
	 */
	public final SPEC roundRobinEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.ROUND_ROBIN;
		return (SPEC) this;
	}

	protected abstract TARGET configure(Reactor reactor, Environment environment);

	@Override
	protected final TARGET configure(Dispatcher dispatcher, Environment environment) {
		return configure(createReactor(dispatcher), environment);
	}

	private Reactor createReactor(Dispatcher dispatcher) {
		return new Reactor(dispatcher, createEventRouter());
	}

	private EventRouter createEventRouter() {
		return new ConsumerFilteringEventRouter(
				eventFilter != null ? eventFilter : createFilter(),
				new ArgumentConvertingConsumerInvoker(converter));
	}

	private Filter createFilter() {
		Filter filter;
		if (EventRoutingStrategy.ROUND_ROBIN == eventRoutingStrategy) {
			filter = new RoundRobinFilter();
		} else if (EventRoutingStrategy.RANDOM == eventRoutingStrategy) {
			filter = new RandomFilter();
		} else if (EventRoutingStrategy.FIRST == eventRoutingStrategy) {
			filter = new FirstFilter();
		} else {
			filter = new PassThroughFilter();
		}
		return filter;
	}

	protected enum EventRoutingStrategy {
		BROADCAST, RANDOM, ROUND_ROBIN, FIRST
	}

}
