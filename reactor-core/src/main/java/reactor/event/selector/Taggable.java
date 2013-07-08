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

package reactor.event.selector;

import java.util.Set;

/**
 * A {@code Taggable} object maintains a set of tags
 *
 * @author Andy Wilkinson
 *
 * @param <T> The taggable type subclass
 */
public interface Taggable<T extends Taggable<T>> {

	/**
	 * Set the set of tags. Wipes out any currently assigned tags.
	 *
	 * @param tags The full set of tags to assign
	 * @return {@literal this}
	 */
	Taggable<T> setTags(String... tags);

	/**
	 * Get the set of currently assigned tags
	 *
	 * @return the tags
	 */
	Set<String> getTags();

	/**
	 * Returns the object that is tagged
	 *
	 * @return the tagged object
	 */
	Object getTagged();

}
