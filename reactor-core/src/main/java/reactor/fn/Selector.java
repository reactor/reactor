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

package reactor.fn;

import java.util.Set;

/**
 * A {@literal Selector} is a wrapper around an arbitrary object as well as provides tagging functionality so that
 * {@literal Selector}s can be filtered based on tags.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public interface Selector {

	/**
	 * Get the unique id of this Selector. Useful for things like caches and referencing in distributed situations.
	 *
	 * @return The unique id.
	 */
	Long getId();

	/**
	 * Get the object being used for comparisons and equals checks.
	 *
	 * @return The internal object.
	 */
	Object getObject();

	/**
	 * Get the set of tags currently assigned to this {@literal Selector}.
	 *
	 * @return
	 */
	Set<String> getTags();

	/**
	 * Set the set of tags to assign to this {@literal Selector}. Wipes out any currently-assigned tags.
	 *
	 * @param tags The full set of tags to assign to this {@literal Selector}.
	 * @return {@literal this}
	 */
	Selector setTags(String... tags);

	/**
	 * Indicates whether this Selector matches the {@code other} Selector.
	 *
	 * @param other The other Selector
	 * @return {@code true} if there's a match, otherwise {@code false}.
	 */
	boolean matches(Selector other);


}
