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

package reactor.fn;

import com.eaio.uuid.UUID;

/**
 * A {@literal Selector} is a wrapper around an arbitrary object. Selectors are {@link Taggable} so that they can be
 * filtered based on their tags.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public interface Selector extends Taggable<Selector> {

	/**
	 * Get the unique id of this Selector. Useful for things like caches and referencing in distributed situations.
	 *
	 * @return The unique id.
	 */
	UUID getId();

	/**
	 * Get the object being used for comparisons and equals checks.
	 *
	 * @return The internal object.
	 */
	Object getObject();

	/**
	 * Indicates whether this Selector matches the {@code object}.
	 *
	 * @param key The key to match
	 * @return {@code true} if there's a match, otherwise {@code false}.
	 */
	boolean matches(Object key);

	/**
	 * Return a component that can resolve headers from a given key.
	 *
	 * @return A {@link HeaderResolver} applicable to this {@link Selector} type.
	 */
	HeaderResolver getHeaderResolver();

}
