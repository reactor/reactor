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

package reactor.bus.routing;

import reactor.core.support.Logger;
import reactor.bus.Event;
import reactor.bus.filter.Filter;
import reactor.bus.registry.Registration;
import reactor.core.error.CancelException;
import reactor.core.support.Assert;
import reactor.core.error.Exceptions;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

import java.util.List;

/**
 * An {@link Router} that {@link Filter#filter filters} consumers before routing events to
 * them.
 *
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class ConsumerFilteringRouter implements Router<Object, Event<?>> {

	private final Logger logger = Logger.getLogger(getClass());
	private final Filter filter;

	/**
	 * Creates a new {@code ConsumerFilteringEventRouter} that will use the {@code filter} to filter consumers.
	 *
	 * @param filter The filter to use. Must not be {@code null}.
	 * @throws IllegalArgumentException if {@code filter} or {@code consumerInvoker} is null.
	 */
	public ConsumerFilteringRouter(Filter filter) {
		Assert.notNull(filter, "filter must not be null");

		this.filter = filter;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E extends Event<?>> void route(Object key, E event,
	                                       List<Registration<Object, ? extends BiConsumer<Object, ? extends Event<?>>>>
	                                         consumers,
	                                       Consumer<E> completionConsumer,
	                                       Consumer<Throwable> errorConsumer) {
		if (null != consumers && !consumers.isEmpty()) {
			List<Registration<Object, ? extends BiConsumer<Object, ? extends Event<?>>>> regs = filter.filter(consumers, key);
			int size = regs.size();
			// old-school for loop is much more efficient than using an iterator
			for (int i = 0; i < size; i++) {
				Registration<Object, ? extends BiConsumer<Object, ? extends Event<?>>> reg = regs.get(i);

				if (null == reg || reg.isCancelled() || reg.isPaused()) {
					continue;
				}
				try {
					((BiConsumer<Object, E>) reg.getObject()).accept(key, event);
				} catch (CancelException cancel) {
					reg.cancel();
				} catch (Throwable t) {
					if (null != errorConsumer) {
						errorConsumer.accept(Exceptions.addValueAsLastCause(t, event));
					} else {
						logger.error("Event routing failed for {}: {}", reg.getObject(), t.getMessage(), t);
						if (RuntimeException.class.isInstance(t)) {
							throw (RuntimeException) t;
						} else {
							throw new IllegalStateException(t);
						}
					}
				} finally {
					if (reg.isCancelAfterUse()) {
						reg.cancel();
					}
				}
			}
		}
		if (null != completionConsumer) {
			try {
				completionConsumer.accept(event);
			} catch (Throwable t) {
				if (null != errorConsumer) {
					errorConsumer.accept(Exceptions.addValueAsLastCause(t, event));
				} else {
					logger.error("Completion Consumer {} failed: {}", completionConsumer, t.getMessage(), t);
				}
			}
		}
	}

	/**
	 * Returns the {@code Filter} being used
	 *
	 * @return The {@code Filter}.
	 */
	public Filter getFilter() {
		return filter;
	}

}
