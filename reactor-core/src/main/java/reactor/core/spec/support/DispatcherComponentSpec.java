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

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.function.Supplier;
import reactor.util.Assert;

/**
 * A generic environment-aware builder for reactor-based components that need to be configured with an {@link
 * Environment} and {@link Dispatcher}.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
@SuppressWarnings("unchecked")
public abstract class DispatcherComponentSpec<SPEC extends DispatcherComponentSpec<SPEC, TARGET>, TARGET> implements Supplier<TARGET> {

	protected Environment env;
	protected Dispatcher  dispatcher;
	protected Reactor     reactor;

	public DispatcherComponentSpec() {
	}

	public DispatcherComponentSpec(DispatcherComponentSpec<?, ?> src) {
		this.env = src.env;
		this.dispatcher = src.dispatcher;
	}

	public SPEC env(Environment env) {
		this.env = env;
		return (SPEC) this;
	}

	public SPEC reactor(Reactor reactor) {
		this.reactor = reactor;
		return (SPEC) this;
	}

	public SPEC defaultDispatcher() {
		Assert.notNull(env, "Cannot use the default Dispatcher without a properly-configured Environment.");
		this.dispatcher = env.getDefaultDispatcher();
		return (SPEC) this;
	}

	public SPEC synchronousDispatcher() {
		this.dispatcher = new SynchronousDispatcher();
		return (SPEC) this;
	}

	public SPEC dispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (SPEC) this;
	}

	public SPEC dispatcher(String dispatcherName) {
		Assert.notNull(env, "Cannot reference a Dispatcher by name without a properly-configured Environment.");
		this.dispatcher = env.getDispatcher(dispatcherName);
		return (SPEC) this;
	}

	@Override
	public final TARGET get() {
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
														null,
														null);
		} else {
			reactor = new Reactor(
					env,
					null == dispatcher ? this.reactor.getDispatcher() : dispatcher,
					null,
					null
			);
		}
		return reactor;
	}

	protected abstract TARGET configure(Reactor reactor);

}
