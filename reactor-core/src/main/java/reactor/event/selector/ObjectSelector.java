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

import reactor.util.UUIDUtils;

import java.util.*;

/**
 * {@link Selector} implementation that uses the {@link #hashCode()} and {@link #equals(Object)}
 * methods of the internal object to determine a match.
 *
 * @param <T> The type of object held by the selector
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class ObjectSelector<T> implements Selector {

	private final UUID   uuid    = UUIDUtils.create();
	private final Object monitor = new Object();

	private final T                 object;
	private       SortedSet<String> tags;

	/**
	 * Create a new {@link Selector} instance from the given object.
	 *
	 * @param object The object to wrap.
	 */
	public ObjectSelector(T object) {
		this.object = object;
	}

	/**
	 * Helper method to create a {@link Selector} from the given object.
	 *
	 * @param obj The object to wrap.
	 * @param <T> The type of the object.
	 * @return The new {@link Selector}.
	 */
	public static <T> Selector objectSelector(T obj) {
		return new ObjectSelector<T>(obj);
	}

	@Override
	public UUID getId() {
		return uuid;
	}

	@Override
	public T getObject() {
		return object;
	}

	@Override
	public Selector setTags(String... tags) {
		synchronized (monitor) {
			this.tags = new TreeSet<String>();
			Collections.addAll(this.tags, tags);
		}
		return this;
	}

	@Override
	public Set<String> getTags() {
		synchronized (monitor) {
			return (null == tags ? Collections.<String>emptySet() : Collections.<String>unmodifiableSet(tags));
		}
	}

	@Override
	public Object getTagged() {
		return object;
	}

	@Override
	public boolean matches(Object key) {
		return !(null == object && null != key) && (object != null && object.equals(key));
	}

	@Override
	public HeaderResolver getHeaderResolver() {
		return null;
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new ObjectSelector<T>(object);
	}

	@Override
	public String toString() {
		synchronized (monitor) {
			return "Selector{" +
					"object=" + object +
					", uuid=" + uuid +
					", tags=" + tags +
					'}';
		}
	}
}
