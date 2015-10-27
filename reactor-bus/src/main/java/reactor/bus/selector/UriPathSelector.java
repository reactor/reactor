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
import java.util.Map;

import reactor.fn.Function;

/**
 * A {@link Selector} implementation based on a {@link UriPathTemplate}.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @see UriPathTemplate
 */
public class UriPathSelector extends ObjectSelector<Object, UriPathTemplate> {

	private final Function<Object, Map<String, Object>> headerResolver = new Function<Object, Map<String, Object>>() {
		@Nullable
		@Override
		public Map<String, Object> apply(Object key) {
			Map<String, Object> headers = getObject().match(key.toString());
			if (null != headers && !headers.isEmpty()) {
				return headers;
			}
			return null;
		}
	};

	/**
	 * Create a selector from the given uri template string.
	 *
	 * @param uriPathTmpl The string to compile into a {@link UriPathTemplate}.
	 */
	public UriPathSelector(String uriPathTmpl) {
		super(new UriPathTemplate(uriPathTmpl));
	}

	/**
	 * Creates a {@link Selector} based on a URI template.
	 *
	 * @param uriTemplate The URI template to compile.
	 * @return The new {@link Selector}.
	 * @see UriPathTemplate
	 */
	public static Selector uriPathSelector(String uriTemplate) {
		return new UriPathSelector(uriTemplate);
	}

	@Override
	public boolean matches(Object path) {
		return String.class.isAssignableFrom(path.getClass()) && getObject().matches((String) path);
	}

	@Override
	public Function<Object, Map<String, Object>> getHeaderResolver() {
		return headerResolver;
	}

}
