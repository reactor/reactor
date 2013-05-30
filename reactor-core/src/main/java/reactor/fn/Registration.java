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

package reactor.fn;

import reactor.fn.selector.Selector;

/**
 * Implementations of this interface provide the ability to manage a {@link reactor.fn.selector.Selector} to {@link Consumer} registration.
 *
 * @author Jon Brisbin
 */
public interface Registration<T> {

	/**
	 * The {@link reactor.fn.selector.Selector} being used in this assignment.
	 *
	 * @return {@literal this}
	 */
	Selector getSelector();

	/**
	 * The item being registered in this assignment.
	 *
	 * @return the registered object
	 */
	T getObject();

	/**
	 * Cancel this {@link Registration} after it has been selected and used. {@link reactor.fn.dispatch.Dispatcher}
	 * implementations should respect this value and perform the cancellation.
	 *
	 * @return
	 */
	Registration<T> cancelAfterUse();

	/**
	 * Whether to cancel this {@link Registration} after use or not.
	 *
	 * @return {@literal true} to cancel after use, {@literal false} otherwise.
	 */
	boolean isCancelAfterUse();

	/**
	 * Cancel this {@literal Registration} by removing it when the registry so it will no longer be assigned.
	 *
	 * @return {@literal this}
	 */
	Registration<T> cancel();

	/**
	 * Has this been cancelled?
	 *
	 * @return {@literal true} if this has been cancelled, {@literal false} otherwise.
	 */
	boolean isCancelled();

	/**
	 * Pause this {@literal Registration}, which means leave it assigned in the registry but skip over it when evaluating
	 * {@literal Consumer}s to be performed for an event.
	 *
	 * @return {@literal this}
	 */
	Registration<T> pause();

	/**
	 * Whether this {@literal Registration} has been paused or not.
	 *
	 * @return {@literal true} if currently paused, {@literal false} otherwise.
	 */
	boolean isPaused();

	/**
	 * Unpause this {@literal Registration}. Does the opposite of {@link #pause()}.
	 *
	 * @return {@literal this}
	 */
	Registration<T> resume();

}
