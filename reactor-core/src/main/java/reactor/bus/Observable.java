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

package reactor.bus;

import reactor.bus.registry.Registration;
import reactor.bus.selector.Selector;
import reactor.fn.Consumer;

/**
 * Basic unit of event handling in Reactor.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public interface Observable<T> {

	/**
	 * Are there any {@link Registration}s with {@link Selector Selectors} that match the given {@code key}.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @return {@literal true} if there are any matching {@literal Registration}s, {@literal false} otherwise
	 */
	boolean respondsToKey(Object key);

	/**
	 * Register a {@link reactor.fn.Consumer} to be triggered when a notification matches the given {@link
	 * Selector}.
	 *
	 * @param selector The {@literal Selector} to be used for matching
	 * @param consumer The {@literal Consumer} to be triggered
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping
	 */
	<V extends T> Registration<Consumer<? extends Event<?>>> on(final Selector selector,
	                                              final Consumer<Event<V>> consumer);


	/**
	 * Notify this component that an {@link Event} is ready to be processed.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @param ev  The {@literal Event}
	 * @return {@literal this}
	 */
	Observable notify(Object key, Event<? extends T> ev);
}
