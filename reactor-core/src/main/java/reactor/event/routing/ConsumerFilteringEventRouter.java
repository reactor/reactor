/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.event.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.filter.Filter;
import reactor.function.Consumer;
import reactor.function.support.CancelConsumerException;
import reactor.util.Assert;

import java.util.List;

/**
 * An {@link reactor.event.routing.EventRouter} that {@link Filter#filter filters} consumers before routing events to
 * them.
 *
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class ConsumerFilteringEventRouter implements EventRouter {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Filter          filter;
	private final ConsumerInvoker consumerInvoker;

	/**
	 * Creates a new {@code ConsumerFilteringEventRouter} that will use the {@code filter} to filter consumers.
	 *
	 * @param filter          The filter to use. Must not be {@code null}.
	 * @param consumerInvoker Used to invoke consumers. Must not be {@code null}.
	 * @throws IllegalArgumentException if {@code filter} or {@code consumerInvoker} is null.
	 */
	public ConsumerFilteringEventRouter(Filter filter, ConsumerInvoker consumerInvoker) {
		Assert.notNull(filter, "filter must not be null");
		Assert.notNull(consumerInvoker, "consumerInvoker must not be null");

		this.filter = filter;
		this.consumerInvoker = consumerInvoker;
	}

	@Override
	public void route(Object key, Event<?> event,
	                  List<Registration<? extends Consumer<? extends Event<?>>>> consumers,
	                  Consumer<?> completionConsumer,
	                  Consumer<Throwable> errorConsumer) {
		if (null != consumers && !consumers.isEmpty()) {
			List<Registration<? extends Consumer<? extends Event<?>>>> regs = filter.filter(consumers, key);
			int size = regs.size();
			// old-school for loop is much more efficient than using an iterator
			for (int i = 0; i < size; i++) {
				Registration<? extends Consumer<? extends Event<?>>> reg = regs.get(i);

				if (reg.isCancelled() || reg.isPaused()) {
					continue;
				}
				try {
					if (null != reg.getSelector().getHeaderResolver()) {
						event.getHeaders().setAll(reg.getSelector().getHeaderResolver().resolve(key));
					}
					consumerInvoker.invoke(reg.getObject(), Void.TYPE, event);
				} catch (CancelConsumerException cancel) {
					reg.cancel();
				} catch (Throwable t) {
					if (null != event.getErrorConsumer()) {
						event.consumeError(t);
					} else if (null != errorConsumer) {
						errorConsumer.accept(t);
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
				consumerInvoker.invoke(completionConsumer, Void.TYPE, event);
			} catch (Exception e) {
				if (null != errorConsumer) {
					errorConsumer.accept(e);
				} else {
					logger.error("Completion Consumer {} failed: {}", completionConsumer, e.getMessage(), e);
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

	/**
	 * Returns the {@code ConsumerInvoker} being used by the event router
	 *
	 * @return The {@code ConsumerInvoker}.
	 */
	public ConsumerInvoker getConsumerInvoker() {
		return consumerInvoker;
	}

}
