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

package reactor.queue;

import reactor.function.Function;
import reactor.function.Supplier;

import javax.annotation.Nonnull;

/**
 * Implementations of this interface are responsible for persisting the elements of a {@link PersistentQueue}.
 * Persistence could be achieved through in-memory solutions like a {@link java.util.Map} or could be more complex and
 * use a backing datastore.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface QueuePersistor<T> extends Iterable<T> {

	/**
	 * Get the value of the last item to have been persisted.
	 *
	 * @return last ID persisted
	 */
	long lastId();

	/**
	 * Get the number of items persisted right now.
	 *
	 * @return number of items persisted
	 */
	long size();

	/**
	 * Are there more items waiting to be returned?
	 *
	 * @return {@code true} if items can be retrieved from this persistor, {@code false} otherwise
	 */
	boolean hasNext();

	/**
	 * Returns a {@link Function} that will persist the item and return a Long id for the item.
	 *
	 * @param t element to persist
	 *
	 * @return id of the item just persisted
	 */
	Long offer(@Nonnull T t);

	/**
	 * Returns a {@link Function} that will return the item with the given id.
	 *
	 * @param idx the given index to lookup
	 *
	 * @return item persisted under the given id
	 */
	T get(Long idx);

	/**
	 * Returns a {@link Supplier} that will simply remove the oldest item from the persistence queue.
	 *
	 * @return the oldest item in the queue
	 */
	T remove();

	/**
	 * Release any internal resources used by the persistence mechanism.
	 */
	void close();

}
