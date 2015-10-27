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

import java.util.Map;

import reactor.fn.Function;

/**
 * Implementation of {@link Selector} that matches
 * all objects.
 *
 * @author Michael Klishin
 */
public class MatchAllSelector implements Selector {

	@Override
	public Object getObject() {
		return null;
	}

	@Override
	public boolean matches(Object key) {
		return true;
	}

	@Override
	public Function<Object, Map<String,Object>> getHeaderResolver() {
		return null;
	}
}
