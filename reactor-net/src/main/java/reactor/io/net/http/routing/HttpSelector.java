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

import reactor.bus.selector.Selector;
import reactor.bus.selector.UriPathSelector;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.model.Method;
import reactor.io.net.http.model.Protocol;

/**
 * A Selector to match against ServerRequest
 *
 * @author Stephane Maldini
 */
public class HttpSelector implements Selector<HttpChannel>, Predicate<HttpChannel> {

	final protected Protocol        protocol;
	final protected Method          method;
	final protected UriPathSelector uriPathSelector;

	@SuppressWarnings("unused")
	public HttpSelector(String uri) {
		this(uri, null, null);
	}

	public HttpSelector(String uri, Protocol protocol, Method method) {
		this.protocol = protocol;
		this.method = method;
		this.uriPathSelector = uri != null && !uri.isEmpty() ? new UriPathSelector(uri) : null;
	}

	@Override
	public Object getObject() {
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Function<Object, Map<String, Object>> getHeaderResolver() {
		return uriPathSelector != null ?
				uriPathSelector.getHeaderResolver() :
				null;
	}

	@Override
	public boolean matches(HttpChannel key) {
		return (protocol == null || protocol.equals(key.protocol()))
		  && (method == null || method.equals(key.method()))
		  && (uriPathSelector == null || uriPathSelector.matches(key.uri()));
	}

	@Override
	public boolean test(HttpChannel channel) {
		return matches(channel);
	}
}
