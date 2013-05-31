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
import reactor.fn.dispatch.ConverterAwareConsumerInvoker;
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

	protected Environment                    env;
	protected Dispatcher                     dispatcher;
	protected Reactor                        reactor;
	protected Converter                      converter;
	protected EventRoutingStrategy           eventRoutingStrategy;
	protected SelectionStrategy              selectionStrategy;

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

	public SPEC threadPoolExecutor() {
		Assert.notNull(env, "Cannot use a thread pool Dispatcher without a properly-configured Environment.");
		this.dispatcher = env.getDispatcher(Environment.THREAD_POOL_EXECUTOR_DISPATCHER);
		return (SPEC) this;
	}

	public SPEC eventLoop() {
		Assert.notNull(env, "Cannot use an event loop Dispatcher without a properly-configured Environment.");
		this.dispatcher = env.getDispatcher(Environment.EVENT_LOOP_DISPATCHER);
		return (SPEC) this;
	}

	public SPEC ringBuffer() {
		Assert.notNull(env, "Cannot use an RingBuffer Dispatcher without a properly-configured Environment.");
		this.dispatcher = env.getDispatcher(Environment.RING_BUFFER_DISPATCHER);
		return (SPEC) this;
	}


	public SPEC dispatcher(String name) {
		Assert.notNull(env, "Cannot use an "+name+" Dispatcher without a properly-configured Environment.");
		this.dispatcher = env.getDispatcher(name);
		return (SPEC) this;
	}

	public TARGET get() {
		return configure(createReactor());
	}

	protected Reactor createReactor() {
		final Reactor reactor;
		if (null == this.dispatcher && env != null){
			this.dispatcher = env.getDispatcher(Environment.DEFAULT_DISPATCHER);
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
			ConsumerInvoker consumerInvoker = new ConverterAwareConsumerInvoker();
			Filter filter = getFilter(((ConsumerFilteringEventRouter) reactor.getEventRouter()).getFilter());
			return new ConsumerFilteringEventRouter(filter, consumerInvoker, converter);
		}
	}

	private EventRouter createEventRouter() {
		return new ConsumerFilteringEventRouter(getFilter(null), new ConverterAwareConsumerInvoker(), converter);
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
