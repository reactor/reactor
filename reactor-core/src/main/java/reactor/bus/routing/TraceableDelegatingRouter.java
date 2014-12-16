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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.bus.registry.Registration;
import reactor.core.support.Assert;
import reactor.fn.Consumer;

import java.util.List;

/**
 * @author Jon Brisbin
 */
public class TraceableDelegatingRouter implements Router {

	private final Router delegate;
	private final Logger log;

	public TraceableDelegatingRouter(Router delegate) {
		Assert.notNull(delegate, "Delegate EventRouter cannot be null.");
		this.delegate = delegate;
		this.log = LoggerFactory.getLogger(delegate.getClass());
	}

	@Override
	public <E> void route(Object key,
	                  E event,
	                  List<Registration<? extends Consumer<?>>> consumers,
	                  Consumer<E> completionConsumer,
	                  Consumer<Throwable> errorConsumer) {
		if(log.isTraceEnabled()) {
			log.trace("route({}, {}, {}, {}, {})", key, event, consumers, completionConsumer, errorConsumer);
		}
		delegate.route(key,event,consumers,completionConsumer,errorConsumer);
	}

}
