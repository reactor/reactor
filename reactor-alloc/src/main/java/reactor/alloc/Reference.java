/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor.alloc;

import reactor.core.support.Recyclable;
import reactor.fn.Supplier;

/**
 * A {@code Reference} provides access to and metadata about a poolable object.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public interface Reference<T extends Recyclable> extends Supplier<T> {

	/**
	 * Get the age of this {@code Reference} since it's creation.
	 *
	 * @return the number of milliseconds since this {@code Reference} was created.
	 */
	long getAge();

	/**
	 * Get the current number of references retained to this object.
	 *
	 * @return the reference count.
	 */
	int getReferenceCount();

	/**
	 * Increase reference count by {@literal 1}.
	 */
	void retain();

	/**
	 * Increase reference count by {@literal incr} amount.
	 *
	 * @param incr
	 * 		the amount to increment the reference count.
	 */
	void retain(int incr);

	/**
	 * Decrease the reference count by {@literal 1}.
	 */
	void release();

	/**
	 * Decrease the reference count by {@literal incr} amount.
	 *
	 * @param decr
	 * 		the amount to decrement the reference count.
	 */
	void release(int decr);

}
