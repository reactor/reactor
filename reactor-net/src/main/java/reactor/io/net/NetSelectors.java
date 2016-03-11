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

package reactor.io.net;

import reactor.bus.selector.Selectors;
import reactor.io.net.http.HttpSelector;
import reactor.io.net.http.model.Method;
import reactor.io.net.http.model.Protocol;

/**
 * Helper methods for creating {@link reactor.bus.selector.Selector}s.
 *
 * @author Stephane Maldini
 */
public abstract class NetSelectors extends Selectors {


	/**
	 * Creates a {@link reactor.bus.selector.Selector} based on a URI template.
	 * This will listen for all Methods.
	 *
	 * @param uri The string to compile into a URI template and use for matching
	 * @return The new {@link HttpSelector}.
	 * @see reactor.bus.selector.UriPathTemplate
	 * @see reactor.bus.selector.UriPathSelector
	 */
	public static HttpSelector http(String uri, Protocol protocol, Method method) {
		if (null == uri) {
			return null;
		}
		return new HttpSelector(uri, protocol, method);
	}

	/**
	 * An alias for {@link reactor.io.net.NetSelectors#http}.
	 * <p>
	 * Creates a {@link reactor.bus.selector.Selector} based on a URI template filtering .
	 * <p>
	 * This will listen for GET Method.
	 *
	 * @param uri The string to compile into a URI template and use for matching
	 * @return The new {@link reactor.bus.selector.UriPathSelector}.
	 * @see reactor.bus.selector.UriPathTemplate
	 * @see reactor.bus.selector.UriPathSelector
	 */
	public static HttpSelector get(String uri) {
		return http(uri, null, Method.GET);
	}

	/**
	 * An alias for {@link reactor.io.net.NetSelectors#http}.
	 * <p>
	 * Creates a {@link reactor.bus.selector.Selector} based on a URI template filtering .
	 * <p>
	 * This will listen for POST Method.
	 *
	 * @param uri The string to compile into a URI template and use for matching
	 * @return The new {@link reactor.bus.selector.UriPathSelector}.
	 * @see reactor.bus.selector.UriPathTemplate
	 * @see reactor.bus.selector.UriPathSelector
	 */
	public static HttpSelector post(String uri) {
		return http(uri, null, Method.POST);
	}

	/**
	 * An alias for {@link reactor.io.net.NetSelectors#http}.
	 * <p>
	 * Creates a {@link reactor.bus.selector.Selector} based on a URI template filtering .
	 * <p>
	 * This will listen for PUT Method.
	 *
	 * @param uri The string to compile into a URI template and use for matching
	 * @return The new {@link reactor.bus.selector.UriPathSelector}.
	 * @see reactor.bus.selector.UriPathTemplate
	 * @see reactor.bus.selector.UriPathSelector
	 */
	public static HttpSelector put(String uri) {
		return http(uri, null, Method.PUT);
	}

	/**
	 * An alias for {@link reactor.io.net.NetSelectors#http}.
	 * <p>
	 * Creates a {@link reactor.bus.selector.Selector} based on a URI template filtering .
	 * <p>
	 * This will listen for DELETE Method.
	 *
	 * @param uri The string to compile into a URI template and use for matching
	 * @return The new {@link reactor.bus.selector.UriPathSelector}.
	 * @see reactor.bus.selector.UriPathTemplate
	 * @see reactor.bus.selector.UriPathSelector
	 */
	public static HttpSelector delete(String uri) {
		return http(uri, null, Method.DELETE);
	}

	/**
	 * An alias for {@link reactor.io.net.NetSelectors#http}.
	 * <p>
	 * Creates a {@link reactor.bus.selector.Selector} based on a URI template filtering .
	 * <p>
	 * This will listen for WebSocket Method.
	 *
	 * @param uri The string to compile into a URI template and use for matching
	 * @return The new {@link reactor.bus.selector.UriPathSelector}.
	 * @see reactor.bus.selector.UriPathTemplate
	 * @see reactor.bus.selector.UriPathSelector
	 */
	public static HttpSelector ws(String uri) {
		return http(uri, null, Method.WS);
	}
}
