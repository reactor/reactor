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

package reactor.event.registry;

import reactor.event.selector.Selector;
import reactor.function.Pausable;

/**
 * A {@code Registration} represents an object that has been {@link Registry#register(Selector,
 * Object) registered} with a {@link Registry}.
 *
 * @param <T> The type of object that is registered
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 *
 */
public interface Registration<T> extends Pausable {

	/**
	 * The {@link reactor.event.selector.Selector} that was used when the registration was made.
	 *
	 * @return the registration's selector
	 */
	Selector getSelector();

	/**
	 * The object that was registered
	 *
	 * @return the registered object
	 */
	T getObject();

	/**
	 * Cancel this {@link Registration} after it has been selected and used. {@link
	 * reactor.core.Dispatcher} implementations should respect this value and perform
	 * the cancellation.
	 *
	 * @return {@literal this}
	 */
	Registration<T> cancelAfterUse();

	/**
	 * Whether to cancel this {@link Registration} after use or not.
	 *
	 * @return {@literal true} if the registration will be cancelled after use, {@literal false}
	 * otherwise.
	 */
	boolean isCancelAfterUse();

	/**
	 * Cancel this {@literal Registration} by removing it from its registry.
	 *
	 * @return {@literal this}
	 */
	@Override
	Registration<T> cancel();

	/**
	 * Has this been cancelled?
	 *
	 * @return {@literal true} if this has been cancelled, {@literal false} otherwise.
	 */
	boolean isCancelled();

	/**
	 * Pause this {@literal Registration}. This leaves it in its registry but, while it is paused, it
	 * will not be eligible for {@link Registry#select(Object) selection}.
	 *
	 * @return {@literal this}
	 */
	@Override
	Registration<T> pause();

	/**
	 * Whether this {@literal Registration} has been paused or not.
	 *
	 * @return {@literal true} if currently paused, {@literal false} otherwise.
	 */
	boolean isPaused();

	/**
	 * Unpause this {@literal Registration}, making it available for {@link Registry#select(Object) selection}.
	 *
	 * @return {@literal this}
	 */
	@Override
	Registration<T> resume();

}
