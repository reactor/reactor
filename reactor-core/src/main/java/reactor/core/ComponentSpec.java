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

import reactor.convert.Converter;
import reactor.convert.DelegatingConverter;
import reactor.fn.registry.Registry;
import reactor.fn.routing.SelectionStrategy;
import reactor.fn.Supplier;
import reactor.fn.routing.TagAwareSelectionStrategy;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.SynchronousDispatcher;
import reactor.util.Assert;

import java.util.List;

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
	protected Registry.LoadBalancingStrategy loadBalancingStrategy;
	protected SelectionStrategy              selectionStrategy;

	public SPEC using(Environment env) {
		this.env = env;
		return (SPEC) this;
	}

	public SPEC using(Reactor reactor) {
		this.reactor = reactor;
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

	public SPEC using(Registry.LoadBalancingStrategy loadBalancingStrategy) {
		this.loadBalancingStrategy = loadBalancingStrategy;
		return (SPEC) this;
	}

	public SPEC broadcastLoadBalancing() {
		this.loadBalancingStrategy = Registry.LoadBalancingStrategy.NONE;
		return (SPEC) this;
	}

	public SPEC randomLoadBalancing() {
		this.loadBalancingStrategy = Registry.LoadBalancingStrategy.RANDOM;
		return (SPEC) this;
	}

	public SPEC roundRobinLoadBalancing() {
		this.loadBalancingStrategy = Registry.LoadBalancingStrategy.ROUND_ROBIN;
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

	public SPEC dispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
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
														loadBalancingStrategy,
														selectionStrategy,
														converter);
		} else {
			reactor = new Reactor(
					env,
					null == dispatcher ? this.reactor.getDispatcher() : dispatcher,
					null == loadBalancingStrategy ? this.reactor.getConsumerRegistry().getLoadBalancingStrategy() : loadBalancingStrategy,
					null == selectionStrategy ? this.reactor.getConsumerRegistry().getSelectionStrategy() : selectionStrategy,
					null == converter ? this.reactor.getConverter() : converter
			);
		}
		return reactor;
	}

	protected abstract TARGET configure(Reactor reactor);

}
