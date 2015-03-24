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

package reactor.bus.ringbuffer;

import reactor.fn.Supplier;

/**
 * A {@link Operation} represents the payload that gets (re) used in the processor.
 *
 * The {@link Operation} itself gets allocated during creation time for the {@link RingBatcher}, so at
 * runtime the instance will be reused. What changes during runtime is the enclosed payload.
 * </p>
 * In addition to the payload, the {@link #id} represents the corresponding ID in the underlying
 * data structure (in the general case a {@link com.lmax.disruptor.RingBuffer}.
 *
 * @param <T> the type of the supplied object
 *
 * @author Jon Brisbin
 */
public abstract class Operation<T> implements Supplier<T> {

	protected volatile Long id;
	private final      T    data;

  /**
   * Create a new {@link Operation} and apply the given payload.
   *
   * @param data the payload to store.
   */
	Operation(T data) {
		this.data = data;
	}

  /**
   * Set the identifier for the underlying payload.
   *
   * @param id the identifier, most likely a sequnce number.
   */
	void setId(Long id) {
		this.id = id;
	}

  /**
   * Get the {@link Operation} payload.
   *
   * @return the enclosed payload.
   */
	@Override public T get() {
		return data;
	}

  /**
   * Commit the {@link Operation} to the underlying datastructure.
   */
	public abstract void commit();

}
