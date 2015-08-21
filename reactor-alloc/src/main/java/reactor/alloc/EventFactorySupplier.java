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

import reactor.bus.Event;
import reactor.fn.Supplier;

/**
 * A {@link Supplier} implementation that instantiates Events
 * based on Event data type.
 *
 * @param <T> type of {@link reactor.bus.Event} data
 * @author Oleksandr Petrov
 * @since 1.1
 */
public class EventFactorySupplier<T> implements Supplier<Event<T>> {

	private final Class<T> klass;

	public EventFactorySupplier(Class<T> klass) {
		this.klass = klass;
	}

	@Override
	public Event<T> get() {
		return new Event<T>(klass);
	}
}
