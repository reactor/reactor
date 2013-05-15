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

package reactor.groovy.support

import groovy.transform.CompileStatic
import reactor.core.Composable
import reactor.fn.Function

/**
 * @author Stephane Maldini
 */
@CompileStatic
class ClosureReduce<T,V> implements Function<Composable.Reduce<T,V>,V>  {

	final Closure<V> callback

	ClosureReduce(Closure<V> cl) {
		callback = cl
	}

	@Override
	V apply(Composable.Reduce<T,V> t) {

		if(t.lastValue)
			callback t.nextValue, t.lastValue
		else
			callback t.nextValue
	}
}
