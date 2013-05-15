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

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import reactor.fn.HeaderResolver;
import reactor.fn.Selector;

import com.eaio.uuid.UUID;

/**
 * {@link reactor.fn.Selector} implementation that uses the {@link #hashCode()} and {@link #equals(Object)} methods of
 * the internal object to determine a match.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class BaseSelector<T> implements Selector {

	private final UUID   uuid    = new UUID();
	private final Object monitor = new Object();

	private final T                 object;
	private       SortedSet<String> tags;

	public BaseSelector(T object) {
		this.object = object;
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
		synchronized(monitor) {
			this.tags = new TreeSet<String>();
			Collections.addAll(this.tags, tags);
		}
		return this;
	}

	@Override
	public Set<String> getTags() {
		synchronized(monitor) {
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
		return new BaseSelector<T>(object);
	}

	@Override
	public String toString() {
		synchronized(monitor) {
			return "Selector{" +
					"object=" + object +
					", uuid=" + uuid +
					", tags=" + tags +
					'}';
		}
	}
}
