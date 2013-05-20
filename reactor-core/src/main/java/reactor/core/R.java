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

package reactor.core;

import reactor.Fn;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Observable;
import reactor.fn.Tuple;

/**
 * Helper class to encapsulate commonly-used functionality around Reactors.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class R {

	private R() {

	}

	/**
	 * Schedule an arbitrary {@link Consumer} to be executed on the given {@link Observable}, passing the given {@link
	 * Event}.
	 *
	 * @param consumer   The {@link Consumer} to invoke.
	 * @param data       The data to pass to the consumer.
	 * @param observable The {@literal Observable} that will be used to invoke the {@literal Consumer}
	 * @param <T>        The type of the data.
	 */
	public static <T> void schedule(final Consumer<T> consumer, T data, Observable observable) {
		observable.notify(Fn.event(Tuple.of(consumer, data)));
	}}
