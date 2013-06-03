/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.core;

import java.util.List;

import reactor.convert.Converter;
import reactor.convert.DelegatingConverter;
import reactor.filter.Filter;
import reactor.filter.PassThroughFilter;
import reactor.filter.RandomFilter;
import reactor.filter.RoundRobinFilter;
import reactor.fn.Supplier;
import reactor.fn.dispatch.ConsumerFilteringEventRouter;
import reactor.fn.dispatch.ConsumerInvoker;
import reactor.fn.dispatch.ArgumentConvertingConsumerInvoker;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.EventRouter;
import reactor.fn.dispatch.SynchronousDispatcher;
import reactor.fn.routing.SelectionStrategy;
import reactor.fn.routing.TagAwareSelectionStrategy;
import reactor.util.Assert;

/**
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
@SuppressWarnings("unchecked")
public abstract class ComponentSpec<SPEC extends ComponentSpec<SPEC, TARGET>, TARGET> implements Supplier<TARGET> {

	protected Environment          env;
	protected Dispatcher           dispatcher;
	protected Reactor              reactor;
	protected Converter            converter;
	protected EventRoutingStrategy eventRoutingStrategy;
	protected SelectionStrategy    selectionStrategy;
	protected String               reactorId;

	public SPEC register() {
		return register("");
	}

	public SPEC register(String id) {
		Assert.notNull(env, "Cannot register reactor without a properly-configured Environment.");
		this.reactorId = id;
		return (SPEC) this;
	}

	public SPEC using(Environment env) {
		this.env = env;
		return (SPEC) this;
	}

	public SPEC using(Reactor reactor) {
		this.reactor = reactor;
		return (SPEC) this;
	}

	public SPEC using(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (SPEC) this;
	}

	public SPEC using(Converter converter) {
		this.converter = converter;
		return (SPEC) this;
	}

	public SPEC using(SelectionStrategy selectionStrategy) {
		this.selectionStrategy = selectionStrategy;
		return (SPEC) this;
	}

	public SPEC using(Converter... converters) {
		this.converter = new DelegatingConverter(converters);
		return (SPEC) this;
	}

	public SPEC using(List<Converter> converters) {
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

	public SPEC roundRobinEventRouting() {
		this.eventRoutingStrategy = EventRoutingStrategy.ROUND_ROBIN;
		return (SPEC) this;
	}

	public SPEC tagFiltering() {
		this.selectionStrategy = new TagAwareSelectionStrategy();
		return (SPEC) this;
	}

	public SPEC sync() {
		this.dispatcher = SynchronousDispatcher.INSTANCE;
		return (SPEC) this;
	}

	public SPEC dispatcher(String dispatcherName) {
		Assert.notNull(env, "Cannot reference a Dispatcher by name without a properly-configured Environment.");
		this.dispatcher = env.getDispatcher(dispatcherName);
		return (SPEC) this;
	}

	public SPEC dispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (SPEC) this;
	}

	public SPEC defaultDispatcher() {
		Assert.notNull(env, "Cannot use the default Dispatcher without a properly-configured Environment.");
		this.dispatcher = env.getDefaultDispatcher();
		return (SPEC) this;
	}

	@Override
	public TARGET get() {
		return configure(createReactor());
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
		if (null != reactorId && env != null){
			env.register(reactorId, reactor);
		}

		return reactor;
	}

	private EventRouter createEventRouter(Reactor reactor) {
		if (converter == null && eventRoutingStrategy == null) {
			return reactor.getEventRouter();
		} else {
			ConsumerInvoker consumerInvoker;
			if (converter == null) {
				consumerInvoker = ((ConsumerFilteringEventRouter)reactor.getEventRouter()).getConsumerInvoker();
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
		} else {
			if (null == existingFilter) {
				filter = new PassThroughFilter();
			} else {
				filter = existingFilter;
			}
		}
		return filter;
	}

	protected abstract TARGET configure(Reactor reactor);

	private enum EventRoutingStrategy {
		BROADCAST, RANDOM, ROUND_ROBIN;
	}
}
