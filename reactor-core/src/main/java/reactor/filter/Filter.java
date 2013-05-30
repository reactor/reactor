/*
 * Copyright (c) 2013 the original author or authors.
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

package reactor.filter;

import java.util.List;

/**
 * A {@code Filter} is used to filter a list of items. The nature of the filtering is determined by the
 * implementation.
 *
 * @author Andy Wilkinson
 *
 */
public interface Filter {

	/**
	 * Filters the given {@code List} of {@code items}. The {@code key} may be used by an implementation to
	 * influence the filtering.
	 *
	 * @param items The items to filter. Must not be {@code null}.
	 * @param key The key
	 *
	 * @return The filtered items, never {@code null}.
	 *
	 * @throws IllegalArgumentException if {@code items} is null
	 */
	<T> List<T> filter(List<T> items, Object key);

}
