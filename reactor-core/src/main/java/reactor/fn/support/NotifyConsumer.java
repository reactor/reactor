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
import reactor.fn.Observable;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class NotifyConsumer<T> implements Consumer<T> {

	private final Object     notifyKey;
	private final Observable observable;

	public NotifyConsumer(Object notifyKey, Observable observable) {
		Assert.notNull(observable, "Observable cannot be null.");
		this.notifyKey = notifyKey;
		this.observable = observable;
	}

	@Override
	public void accept(T t) {
		Event<T> ev = Event.wrap(t);
		if (null == notifyKey) {
			observable.notify(ev);
		} else {
			observable.notify(notifyKey, ev);
		}
	}

}
