/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package reactor.fn.selector;

import reactor.fn.Selector;
import reactor.fn.support.UriTemplate;

/**
 * A {@link reactor.fn.Selector} implementation based on the given string that is compiled into a {@link UriTemplate}.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 *
 * @see {@link UriTemplate}
 */
public class UriTemplateSelector extends BaseSelector<UriTemplate> {

	/**
	 * Create a selector from the given uri template string.
	 *
	 * @param urit The string to compile into a {@link UriTemplate}.
	 */
	public UriTemplateSelector(String urit) {
		super(new UriTemplate(urit));
	}

	@Override
	public boolean matches(Selector s2) {
		if (!(s2.getObject() instanceof String)) {
			return false;
		}

		String path = (String) s2.getObject();
		return getObject().matches(path);
	}
}
