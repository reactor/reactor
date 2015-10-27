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
package reactor.io.net.http.routing;

import java.util.Map;

import reactor.bus.registry.Registries;
import reactor.bus.registry.Registry;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;
import reactor.core.publisher.convert.DependencyUtils;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.http.HttpChannel;

/**
 * @author Stephane Maldini
 */
public class RegistryChannelMappings<IN, OUT> extends ChannelMappings<IN, OUT> {

	static {
		if (!DependencyUtils.hasReactorCodec()) {
			throw new IllegalStateException("io.projectreactor:reactor-bus:" + DependencyUtils.reactorVersion() +
					" dependency is missing from the classpath.");
		}
	}

	private final Registry<HttpChannel<IN, OUT>, HttpHandlerMapping<IN, OUT>> routedWriters =
			Registries.create();

	@Override
	public Iterable<? extends ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>> apply(
			HttpChannel<IN, OUT> channel) {
		return routedWriters.selectValues(channel);
	}

	@Override
	@SuppressWarnings("unchecked")
	public ChannelMappings<IN, OUT> add(Predicate<? super HttpChannel<IN, OUT>> condition,
			ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {

		Selector<HttpChannel<IN, OUT>> selector = Selector.class.isAssignableFrom(condition.getClass()) ?
				(Selector<HttpChannel<IN,OUT>>)condition :
				Selectors.predicate(condition);

		routedWriters.register(selector, new HttpHandlerMapping<>(condition, handler, selector.getHeaderResolver()));

		return this;
	}
}
