/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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
import reactor.core.Reactor
import reactor.function.Consumer
import reactor.event.Event
import reactor.function.support.CancelConsumerException

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@CompileStatic
class ClosureEventConsumer<T> implements Consumer<Event<T>> {

	final Closure callback
	final boolean eventArg

	ClosureEventConsumer(Closure cl) {
		callback = cl
		callback.delegate = this
		def argTypes = callback.parameterTypes
		this.eventArg = Event.isAssignableFrom(argTypes[0])
	}

	void cancel() {
		throw new CancelConsumerException()
	}

	@Override
	void accept(Event<T> arg) {
		def callback = this.callback
		if (Reactor.ReplyToEvent.class.isAssignableFrom(arg.class)) {
			callback = (Closure) callback.clone()
			callback.delegate = new ReplyDecorator(arg.replyTo, (((Reactor.ReplyToEvent) arg).replyToObservable))
		}
		if (eventArg) {
			callback arg
		} else {
			callback arg?.data
		}
	}

	class ReplyDecorator {

		final replyTo
		final reactor.core.Observable observable

		ReplyDecorator(replyTo, reactor.core.Observable observable) {
			this.replyTo = replyTo
			this.observable = observable
		}


		void reply() {
			observable.notify(replyTo, new Event<Void>(Void))
		}

		void reply(data) {
			observable.notify(replyTo, Event.wrap(data))
		}
	}
}
