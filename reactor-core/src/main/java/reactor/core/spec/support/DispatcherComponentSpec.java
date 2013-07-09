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

	private Environment env;
	private Dispatcher  dispatcher;

	public final SPEC env(Environment env) {
		this.env = env;
		return (SPEC) this;
	}

	public final SPEC defaultDispatcher() {
		Assert.notNull(env, "Cannot use the default Dispatcher without a properly-configured Environment.");
		this.dispatcher = env.getDefaultDispatcher();
		return (SPEC) this;
	}

	public final SPEC synchronousDispatcher() {
		this.dispatcher = new SynchronousDispatcher();
		return (SPEC) this;
	}

	public final SPEC dispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (SPEC) this;
	}

	public final SPEC dispatcher(String dispatcherName) {
		Assert.notNull(env, "Cannot reference a Dispatcher by name without a properly-configured Environment.");
		this.dispatcher = env.getDispatcher(dispatcherName);
		return (SPEC) this;
	}

	@Override
	public final TARGET get() {
		return configure(getDispatcher(), this.env);
	}

	private final Dispatcher getDispatcher() {
		if (this.dispatcher != null) {
			return this.dispatcher;
		} else if (env != null) {
			return env.getDefaultDispatcher();
		} else {
			return new SynchronousDispatcher();
		}
	}

	protected abstract TARGET configure(Dispatcher dispatcher, Environment environment);

}
