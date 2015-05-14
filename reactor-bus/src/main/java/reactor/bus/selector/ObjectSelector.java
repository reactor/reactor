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

import reactor.fn.Predicate;

/**
 * {@link Selector} implementation that uses the {@link #hashCode()} and {@link #equals(Object)}
 * methods of the internal object to determine a match.
 *
 * @param <K>
 * 		The type of object held by the selector
 * @param <T>
 * 		The type of object held by the selector
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class ObjectSelector<K, T> implements Selector<K>, Predicate<K> {

	private final Object monitor = new Object();
	private final T object;

	/**
	 * Create a new {@link Selector} instance from the given object.
	 *
	 * @param object
	 * 		The object to wrap.
	 */
	public ObjectSelector(T object) {
		this.object = object;
	}

	/**
	 * Helper method to create a {@link Selector} from the given object.
	 *
	 * @param obj
	 * 		The object to wrap.
	 * @param <T>
	 * 		The type of the object.
	 *
	 * @return The new {@link Selector}.
	 */
	public static <T> Selector<T> objectSelector(T obj) {
		return new ObjectSelector<T, T>(obj);
	}

	@Override
	public T getObject() {
		return object;
	}

	@Override
	public boolean matches(K key) {
		return !(null == object && null != key) && (object != null && object.equals(key));
	}

	@Override
	public HeaderResolver getHeaderResolver() {
		return null;
	}

	@Override
	public boolean test(K t) {
		return matches(t);
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new ObjectSelector<K, T>(object);
	}

	@Override
	public String toString() {
		synchronized(monitor) {
			return "Selector{" +
					"object=" + object +
					'}';
		}
	}

}
