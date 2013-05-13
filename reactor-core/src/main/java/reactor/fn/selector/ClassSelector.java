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

package reactor.fn.selector;


/**
 * Implementation of {@link reactor.fn.Selector} that uses {@link Class#isAssignableFrom(Class)} to determine a match.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class ClassSelector extends BaseSelector<Class<?>> {

	public ClassSelector(Class<?> type) {
		super(type);
	}

	@Override
	public boolean matches(Object key) {
		if (!(key instanceof Class)) {
			return false;
		}
		return getObject().isAssignableFrom((Class<?>)key);
	}

}
