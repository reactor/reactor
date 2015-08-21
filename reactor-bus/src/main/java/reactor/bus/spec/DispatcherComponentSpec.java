/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.bus.spec;

import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.fn.Supplier;

/**
 * A generic environment-aware class for specifying components that need to be configured
 * with an {@link Environment} and {@link ReactorProcessor}.
 *
 * @param <SPEC>   The DispatcherComponentSpec subclass
 * @param <TARGET> The type that this spec will create
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
@SuppressWarnings("unchecked")
public abstract class DispatcherComponentSpec<SPEC extends DispatcherComponentSpec<SPEC, TARGET>, TARGET> implements
  Supplier<TARGET> {


	private Environment      env;
	private ReactorProcessor dispatcher;

	/**
	 * Configures the spec, and potentially the component being configured, to use the given
	 * environment
	 *
	 * @param env The environment to use
	 * @return {@code this}
	 */
	public final SPEC env(Environment env) {
		this.env = env;
		return (SPEC) this;
	}

	/**
	 * Configures the component to use the configured Environment's default dispatcher
	 *
	 * @return {@code this}
	 * @throws IllegalStateException if no Environment has been configured
	 * @see Environment#getDefaultDispatcher()
	 * @see #env(Environment)
	 */
	public final SPEC defaultDispatcher() {
		assertNonNullEnvironment("Cannot use the default Dispatcher without an Environment");
		this.dispatcher = env.getDefaultDispatcher();
		return (SPEC) this;
	}

	/**
	 * Configures the component to use a synchronous dispatcher
	 *
	 * @return {@code this}
	 */
	public final SPEC synchronousDispatcher() {
		this.dispatcher = SynchronousDispatcher.INSTANCE;
		return (SPEC) this;
	}

	/**
	 * Configures the component to use the given {@code dispatcher}
	 *
	 * @param dispatcher The dispatcher to use
	 * @return {@code this}
	 */
	public final SPEC dispatcher(ReactorProcessor dispatcher) {
		this.dispatcher = dispatcher;
		return (SPEC) this;
	}

	/**
	 * Configures the component to the dispatcher in the configured Environment with the given
	 * {@code dispatcherName}
	 *
	 * @param dispatcherName The name of the dispatcher
	 * @return {@code this}
	 * @throws IllegalStateException if no Environment has been configured
	 * @see Environment#getDispatcher(String)
	 * @see #env(Environment)
	 */
	public final SPEC dispatcher(String dispatcherName) {
		assertNonNullEnvironment("Cannot reference a Dispatcher by name without an Environment");
		this.dispatcher = env.getDispatcher(dispatcherName);
		return (SPEC) this;
	}

	@Override
	public final TARGET get() {
		return configure(getDispatcher(), this.env);
	}

	private ReactorProcessor getDispatcher() {
		if (this.dispatcher != null) {
			return this.dispatcher;
		} else if (env != null) {
			return env.getDefaultDispatcher();
		} else {
			return SynchronousDispatcher.INSTANCE;
		}
	}

	private void assertNonNullEnvironment(String message) {
		if (env == null) {
			throw new IllegalStateException(message);
		}
	}

	protected abstract TARGET configure(ReactorProcessor dispatcher, Environment environment);

}
