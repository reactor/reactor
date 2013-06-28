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

package reactor.fn.support;

import reactor.fn.Consumer;
import reactor.fn.Event;

/**
 * Simple {@link Consumer} implementation that pulls the data from an {@link Event} and passes it to a delegate {@link
 * Consumer}.
 *
 * @author Jon Brisbin
 */
public class EventConsumer<T> implements Consumer<Event<T>> {

	private final Consumer<T> delegate;

	public EventConsumer(Consumer<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public void accept(Event<T> ev) {
		if (null != delegate) {
			delegate.accept(ev.getData());
		}
	}

}
