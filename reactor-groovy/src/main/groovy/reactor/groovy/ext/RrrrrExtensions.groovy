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
import reactor.core.R
import reactor.fn.Event
import reactor.fn.Observable
import reactor.fn.Selector
import reactor.groovy.support.ClosureConsumer
import reactor.groovy.support.ClosureEventConsumer

/**
 * @author Jon Brisbin
 */
@CompileStatic
class RrrrrExtensions {

	/**
	 * Closure converters
	 */
	static <T> void schedule(final R selfType, final T value, final Observable observable, final Closure closure) {
		R.schedule new ClosureConsumer(closure), value, observable
	}
}
