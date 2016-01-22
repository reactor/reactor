/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.List;

import reactor.core.trait.Recyclable;

/**
 * An {@code Allocator} is responsible for returning to the caller a {@link Reference} to a reusable
 * object or to provide a newly-created object, depending on the underlying allocation strategy.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public interface Allocator<T extends Recyclable> {

	/**
	 * Allocate an object from the internal pool.
	 *
	 * @return a {@link Reference} that can be retained and released.
	 */
	Reference<T> allocate();

	/**
	 * Allocate a batch of objects all at once.
	 *
	 * @param size the number of objects to allocate
	 * @return a {@code List} of {@link Reference References} to the allocated objects
	 */
	List<Reference<T>> allocateBatch(int size);

	/**
	 * Efficiently release a batch of References all at once.
	 *
	 * @param batch the references to release
	 */
	void release(List<Reference<T>> batch);

}
