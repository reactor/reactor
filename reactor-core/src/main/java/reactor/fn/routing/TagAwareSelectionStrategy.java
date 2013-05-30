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

package reactor.fn.routing;

import reactor.fn.Selector;
import reactor.fn.selector.Taggable;

import java.util.Set;

/**
 * Matches a {@link reactor.fn.Selector} and key like normal, but only if the key is {@link reactor.fn.selector.Taggable} and they have at least one tag in common.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class TagAwareSelectionStrategy implements SelectionStrategy {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public boolean matches(Selector selector, Object key) {
		Taggable taggableKey = (Taggable) key;
		if (selector.matches(taggableKey.getTagged())) {
			Set<String> tags = taggableKey.getTags();
			for (String keyTag : tags) {
				for (String selectorTag : selector.getTags()) {
					if (selectorTag.equals(keyTag)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	@Override
	public boolean supports(Object sel) {
		return sel instanceof Taggable;
	}

}
