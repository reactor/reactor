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

package reactor.fn.registry;

import java.util.List;

import reactor.fn.routing.SelectionStrategy;
import reactor.fn.selector.Selector;

/**
 * Implementations of this interface manage a registry of objects that works sort of like a Map, except Registries don't
 * use simple keys, they use {@link reactor.fn.selector.Selector}s to map their objects.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public interface Registry<T> extends Iterable<Registration<? extends T>> {

	/**
	 * Assign the given {@link reactor.fn.selector.Selector} with the given object.
	 *
	 * @param sel The left-hand side of the {@literal Selector} comparison check.
	 * @param obj The object to assign.
	 * @return {@literal this}
	 */
	<V extends T> Registration<V> register(Selector sel, V obj);

	/**
	 * Remove any objects matching this {@code key}. This will unregister <b>all</b> objects matching the given
	 * {@literal key}. There's no provision for removing only a specific object.
	 *
	 * @param key The key to be matched by the Selectors
	 * @return {@literal true} if any objects were unassigned, {@literal false} otherwise.
	 */
	boolean unregister(Object key);

	/**
	 * Select {@link Registration}s whose {@link Selector} {@link Selector#matches(Object)} the given {@code key}.
	 *
	 * @param key The key for the Selectors to match
	 * @return A {@link List} of {@link Registration}s whose {@link Selector} matches the given key.
	 */
	List<Registration<? extends T>> select(Object key);

	/**
	 * Returns the custom selection strategy, if any, that is being used by this {@literal Registry}
	 *
	 * @return the {@link reactor.fn.routing.SelectionStrategy}. May be {@code null}.
	 */
	SelectionStrategy getSelectionStrategy();
}
