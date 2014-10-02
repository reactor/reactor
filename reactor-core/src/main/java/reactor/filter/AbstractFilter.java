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

package reactor.filter;

import java.util.List;

import reactor.util.Assert;

abstract class AbstractFilter implements Filter {

	@Override
	public final <T> List<T> filter(List<T> items, Object key) {
		Assert.notNull(items, "items must not be null");
		return doFilter(items, key);
	}

	protected abstract <T> List<T> doFilter(List<T> items, Object key);

}
