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
package reactor.groovy.ext

import groovy.transform.CompileStatic
import reactor.core.Reactor
import reactor.fn.Linkable


/**
 * @author Stephane Maldini (smaldini)
 */
@CompileStatic
class LinkableExtensions {

	static <T> Linkable<T> minus(final Linkable<T> selfType, T... linkables) {
		for (T linkable in linkables)
			selfType.unlink linkable
		selfType
	}

	static <T extends Linkable<T>> T or(final Linkable<T> selfType, T linkable) {
		selfType.link linkable
		linkable
	}

	static <T> Linkable<T> plus(final Linkable<T> selfType, T linkable) {
		selfType.link linkable
		selfType
	}

	static <T> Collection<T> or(final Linkable<T> selfType, Collection<T> linkables) {
		for (T linkable in linkables)
			selfType.link linkable
		linkables
	}

}
