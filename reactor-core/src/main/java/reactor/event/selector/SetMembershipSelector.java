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

package reactor.event.selector;

import java.util.Set;

/**
 * Implementation of {@link reactor.event.selector.Selector} that matches
 * objects on set membership.
 *
 * @author Michael Klishin
 */
public class SetMembershipSelector implements Selector {
	private final Set set;

	/**
	 * Create a {@link Selector} when the given regex pattern.
	 *
	 * @param set
	 * 		The {@link Set} that will be used for membership checks.
	 */
	public SetMembershipSelector(Set set) {
		this.set = set;
	}

	@Override
	public Object getObject() {
		return this.set;
	}

	@Override
	public boolean matches(Object key) {
		return this.set.contains(key);
	}

	@Override
	public HeaderResolver getHeaderResolver() {
		return null;
	}
}
