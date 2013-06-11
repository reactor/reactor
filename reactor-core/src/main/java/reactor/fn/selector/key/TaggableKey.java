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

package reactor.fn.selector.key;

import reactor.fn.selector.Taggable;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * A helper class to make a notification key {@link reactor.fn.selector.Taggable}.
 *
 * @author Andy Wilkinson
 */
public final class TaggableKey implements Taggable<TaggableKey> {

	private final Object monitor = new Object();

	private final Object delegate;

	private Set<String> tags = new TreeSet<String>();

	/**
	 * Creates a {@link Taggable} notification key that will defer to the
	 * {@code delegate} for {@link #hashCode} and {@link #equals}.
	 *
	 * @param delegate The delegate
	 */
	public TaggableKey(Object delegate) {
		this.delegate = delegate;
	}

	@Override
	public Taggable<TaggableKey> setTags(String... tags) {
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
		return delegate;
	}

	@Override
	public boolean equals(Object other) {
		return this.delegate.equals(other);
	}

	@Override
	public int hashCode() {
		return this.delegate.hashCode();
	}
}
