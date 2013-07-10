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
import reactor.filter.Filter;
import reactor.function.Consumer;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.util.Assert;

import java.util.List;

/**
 * An {@link reactor.event.routing.EventRouter} that {@link Filter#filter filters} consumers before
 * routing events to them.
 *
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public final class ConsumerFilteringEventRouter implements EventRouter {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Filter          filter;
	private final ConsumerInvoker consumerInvoker;

	/**
	 * Creates a new {@code ConsumerFilteringEventRouter} that will use the {@code filter} to
	 * filter consumers.
	 *
	 * @param filter          The filter to use. Must not be {@code null}.
	 * @param consumerInvoker Used to invoke consumers. Must not be {@code null}.
	 *
	 * @throws IllegalArgumentException if {@code filter} or {@code consumerInvoker} is null.
	 */
	public ConsumerFilteringEventRouter(Filter filter, ConsumerInvoker consumerInvoker) {
		Assert.notNull(filter, "filter must not be null");
		Assert.notNull(consumerInvoker, "consumerInvoker must not be null");

		this.filter = filter;
		this.consumerInvoker = consumerInvoker;
	}

	@Override
	public void route(Object key, Event<?> event, List<Registration<? extends Consumer<? extends Event<?>>>> consumers, Consumer<?> completionConsumer, Consumer<Throwable> errorConsumer) {
		try {
			for (Registration<? extends Consumer<? extends Event<?>>> consumer : filter.filter(consumers, key)) {
				invokeConsumer(key, event, consumer);
				if (null != completionConsumer) {
					consumerInvoker.invoke(completionConsumer, Void.TYPE, event);
				}
			}
		} catch (Exception e) {
			if (null != errorConsumer) {
				errorConsumer.accept(e);
			}else{
				logger.error("Event routing failed: {}", e.getMessage(), e);
			}
		}
	}

	protected void invokeConsumer(Object key, Event<?> event, Registration<? extends Consumer<? extends Event<?>>> registeredConsumer) throws Exception {
		if (isRegistrationActive(registeredConsumer)) {
			if (null != registeredConsumer.getSelector().getHeaderResolver()) {
				event.getHeaders().setAll(registeredConsumer.getSelector().getHeaderResolver().resolve(key));
			}
			consumerInvoker.invoke(registeredConsumer.getObject(), Void.TYPE, event);
			if (registeredConsumer.isCancelAfterUse()) {
				registeredConsumer.cancel();
			}
		}
	}

	private boolean isRegistrationActive(Registration<?> registration) {
		return (!registration.isCancelled() && !registration.isPaused());
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
