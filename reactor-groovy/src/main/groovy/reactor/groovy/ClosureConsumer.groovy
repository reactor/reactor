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

package reactor.groovy

import groovy.transform.CompileStatic
import reactor.Fn
import reactor.core.R
import reactor.core.R
import reactor.fn.Consumer
import reactor.fn.Event

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@CompileStatic
class ClosureConsumer<T> implements Consumer<T> {

	final Closure callback
	final Class[] argTypes

	Event currentEvent

	ClosureConsumer(Closure cl) {
		callback = cl
		argTypes = callback.parameterTypes
		callback.resolveStrategy = Closure.DELEGATE_FIRST
	}

	@Override
	void accept(T arg) {
		def isEvent = arg instanceof Event
		if (isEvent) {
			currentEvent = (Event) arg
		}

		if (!argTypes) {
			callback()
			return
		}

		if (arg
				&& argTypes[0] != Object
				&& !argTypes[0].isAssignableFrom(arg.class)
				&& isEvent) {
			callback(currentEvent.data)
		} else {
			callback arg
		}
	}

	void reply(Object replyData) {
		reply(Fn.event(replyData))
	}

	void reply(Event<?> replyEvent) {
		R.replyTo currentEvent, replyEvent
	}

//	@Override
//	protected Object clone() throws CloneNotSupportedException {
//		return super.clone()
//	}

}
