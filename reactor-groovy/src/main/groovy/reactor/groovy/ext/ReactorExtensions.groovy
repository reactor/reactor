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
import reactor.Fn
import reactor.core.Composable
import reactor.core.Promise
import reactor.core.R
import reactor.fn.Consumer
import reactor.fn.Function
import reactor.fn.Observable
import reactor.fn.Supplier
import reactor.groovy.support.ClosureConsumer
import reactor.groovy.support.ClosureSupplier
/**
 * @author Stephane Maldini
 */
@CompileStatic
class ReactorExtensions {

	/**
	 * Closure converters
	 */
	static <T> void schedule(final Fn selfType, final T value, final Observable observable, final Closure closure) {
		Fn.schedule new ClosureConsumer(closure), value, observable
	}

	static <T> Composable.Builder<T> compose(final Fn selfType, Closure<T> callback) {
		Fn.compose new ClosureSupplier<T>(callback)
	}

	static <T> Promise.Builder<T> promise(final Fn selfType, Closure<T> callback) {
		Fn.promise new ClosureSupplier<T>(callback)
	}
}
