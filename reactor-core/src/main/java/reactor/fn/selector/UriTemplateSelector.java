/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.fn.selector;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * A {@link Selector} implementation based on the given string that is compiled into a {@link UriTemplate}.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @see {@link UriTemplate}
 */
public class UriTemplateSelector extends BaseSelector<UriTemplate> {

	private final HeaderResolver headerResolver = new HeaderResolver() {
		@Nullable
		@Override
		public Map<String, String> resolve(Object key) {
			Map<String, String> headers = getObject().match(key.toString());
			if (null != headers && !headers.isEmpty()) {
				return headers;
			}
			return null;
		}
	};

	/**
	 * Create a selector when the given uri template string.
	 *
	 * @param tmpl The string to compile into a {@link UriTemplate}.
	 */
	public UriTemplateSelector(String tmpl) {
		super(new UriTemplate(tmpl));
	}

	@Override
	public boolean matches(Object key) {
		if (!(key instanceof String)) {
			return false;
		}

		String path = (String) key;
		return getObject().matches(path);
	}

	@Override
	public HeaderResolver getHeaderResolver() {
		return headerResolver;
	}

}
