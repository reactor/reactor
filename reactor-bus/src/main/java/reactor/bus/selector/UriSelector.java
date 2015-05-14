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

package reactor.bus.selector;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Selector} implementation that matches on various components of a full URI.
 * <p>
 * The {@code UriSelector} will explode a URI into its component parts. If you use the following as a selector:
 * <pre><code>
 * reactor.on(U("scheme://host/path"), ...);
 * </code></pre>
 * Then any {@link java.net.URI} or String that matches those elements will be matched.
 * </p>
 * <p>
 * It's not an exact string match but a match on components because you can pass parameters in the form of a query
 * string. Consider the following example:
 * <pre><code>
 * reactor.on(U("tcp://*:3000/topic"), consumer);
 * </code></pre>
 * This means "match any URIs using scheme 'tcp' to port '3000' for the path '/topic' irregardless of host". When a URI
 * is sent as the key to a notify like this:
 * <pre><code>
 * reactor.notify("tcp://user:ENCODEDPWD@192.168.0.10:3000/topic?param=value"), event);
 * </code></pre>
 * The match will succeed and the headers will be set as follows:
 * <ul>
 * <li>{@code scheme}: "tcp"</li>
 * <li>{@code authorization}: "user:ENCODEDPWD@192.168.0.10:3000"</li>
 * <li>{@code userInfo}: "user:ENCODEDPWD"</li>
 * <li>{@code host}: "192.168.0.10"</li>
 * <li>{@code port}: "3000"</li>
 * <li>{@code path}: "/topic"</li>
 * <li>{@code fragment}: {@code null}</li>
 * <li>{@code query}: "param=value"</li>
 * <li>{@code param}: "value"</li>
 * </ul>
 * </p>
 * <p>
 * Wildcards can be used in place of {@code host}, and {@code path}. The former by replacing the host with '*' and the
 * latter by replacing the path with '/*'.
 * </p>
 *
 * @author Jon Brisbin
 */
public class UriSelector extends ObjectSelector<Object, URI> {

	private static final UriHeaderResolver URI_HEADER_RESOLVER = new UriHeaderResolver();

	private final String scheme;
	private final String host;
	private final int    port;
	private final String path;
	private final String fragment;

	public UriSelector(String uri) {
		this(URI.create(uri));
	}

	public UriSelector(URI uri) {
		super(uri);
		scheme = (null != uri.getScheme() ? uri.getScheme() : "*");
		String authority = uri.getAuthority();
		host = (null != uri.getHost() ? uri.getHost() : "*");
		if(authority.contains("*:")) {
			int i = authority.lastIndexOf(":") + 1;
			if(i > 1) {
				port = Integer.parseInt(authority.substring(i));
			} else {
				port = -1;
			}
		} else {
			port = uri.getPort();
		}
		path = (null != uri.getPath() ? uri.getPath() : "/*");
		fragment = uri.getFragment();
	}

	@Override
	public HeaderResolver getHeaderResolver() {
		return URI_HEADER_RESOLVER;
	}

	@Override
	public boolean matches(Object key) {
		if(null == key) {
			return false;
		}

		URI uri = objectToURI(key);

		if(uri == null){
			return false;
		}

		boolean schemeMatches = "*".equals(scheme) || scheme.equals(uri.getScheme());
		boolean hostMatches = "*".equals(host) || host.equals(uri.getHost());
		boolean portMatches = -1 == port || port == uri.getPort();
		boolean pathMatches = "/*".equals(path) || path.equals(uri.getPath());
		boolean fragmentMatches = null == fragment || fragment.equals(uri.getFragment());

		return schemeMatches
				&& hostMatches
				&& portMatches
				&& pathMatches
				&& fragmentMatches;
	}

	private static URI objectToURI(Object key) {
		if(key instanceof URI) {
			return (URI)key;
		} else if(key instanceof String) {
			return URI.create(key.toString());
		} else {
			return null;
		}
	}

	private static class UriHeaderResolver implements HeaderResolver {
		@Nullable
		@Override
		public Map<String, Object> resolve(Object key) {
			if(null == key) {
				return null;
			}

			URI uri = objectToURI(key);

			if(uri == null){
				return null;
			}

			Map<String, Object> headers = new HashMap<String, Object>();

			headers.put("authority", uri.getAuthority());
			headers.put("fragment", uri.getFragment());
			headers.put("host", uri.getHost());
			headers.put("path", uri.getPath());
			headers.put("port", String.valueOf(uri.getPort()));
			headers.put("query", uri.getQuery());
			if(null != uri.getQuery()) {
				try {
					String query = URLDecoder.decode(uri.getQuery(), "ISO-8859-1");
					for(String s : query.split("&")) {
						String[] parts = s.split("=");
						headers.put(parts[0], parts[1]);
					}
				} catch(UnsupportedEncodingException e) {
					throw new IllegalArgumentException(e);
				}
			}
			headers.put("scheme", uri.getScheme());
			headers.put("userInfo", uri.getUserInfo());

			return headers;
		}
	}

}
