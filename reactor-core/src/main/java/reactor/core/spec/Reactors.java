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

package reactor.core.spec;

import reactor.core.Environment;
import reactor.core.Observable;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.tuple.Tuple;

/**
 * Base class to encapsulate commonly-used functionality around Reactors.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public abstract class Reactors {

	/**
	 * Create a new {@link ReactorSpec} to configure a Reactor.
	 *
	 * @return The Reactor spec
	 */
	public static ReactorSpec reactor() {
		return new ReactorSpec();
	}

	/**
	 * Create a new {@link reactor.core.Reactor} using the given {@link reactor.core.Environment}.
	 *
	 * @param env
	 * 		The {@link reactor.core.Environment} to use.
	 *
	 * @return A new {@link reactor.core.Reactor}
	 */
	public static Reactor reactor(Environment env) {
		return new ReactorSpec().env(env).dispatcher(env.getDefaultDispatcher()).get();
	}

	/**
	 * Create a new {@link reactor.core.Reactor} using the given {@link reactor.core.Environment} and dispatcher name.
	 *
	 * @param env
	 * 		The {@link reactor.core.Environment} to use.
	 * @param dispatcher
	 * 		The name of the {@link reactor.event.dispatch.Dispatcher} to use.
	 *
	 * @return A new {@link reactor.core.Reactor}
	 */
	public static Reactor reactor(Environment env, String dispatcher) {
		return new ReactorSpec().env(env).dispatcher(dispatcher).get();
	}

	/**
	 * Create a new {@link reactor.core.Reactor} using the given {@link reactor.core.Environment} and {@link
	 * reactor.event.dispatch.Dispatcher}.
	 *
	 * @param env
	 * 		The {@link reactor.core.Environment} to use.
	 * @param dispatcher
	 * 		The {@link reactor.event.dispatch.Dispatcher} to use.
	 *
	 * @return A new {@link reactor.core.Reactor}
	 */
	public static Reactor reactor(Environment env, Dispatcher dispatcher) {
		return new ReactorSpec().env(env).dispatcher(dispatcher).get();
	}

	/**
	 * Schedule an arbitrary {@link reactor.function.Consumer} to be executed on the given {@link
	 * reactor.core.Observable}, passing the given {@link
	 * reactor.event.Event}.
	 *
	 * @param consumer
	 * 		The {@link reactor.function.Consumer} to invoke.
	 * @param data
	 * 		The data to pass to the consumer.
	 * @param observable
	 * 		The {@literal Observable} that will be used to invoke the {@literal Consumer}
	 * @param <T>
	 * 		The type of the data.
	 */
	public static <T> void schedule(final Consumer<T> consumer, T data, Observable observable) {
		observable.notify(Event.wrap(Tuple.of(consumer, data)));
	}

}
