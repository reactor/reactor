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
package reactor.actions;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.function.Consumer;

/**
 * Base class for all Composable actions such as map, reduce, filter...
 *
 * @param <T>
 * 		The type of the values
 *
 * @author Stephane Maldini
 */
public abstract class Action<T> implements Consumer<Event<T>> {

	private final Observable observable;
	private final Object successKey;
	private final Object failureKey;

	@Override
	public void accept(Event<T> tEvent) {
		try {
			doOperation(tEvent);
		} catch (Throwable e) {
			notifyError(e);
		}
	}

	public Observable getObservable() {
		return observable;
	}

	public Object getSuccessKey() {
		return successKey;
	}
	public Object getFailureKey() {
		return failureKey;
	}

	protected Action(Observable observable, Object successKey, Object failureKey) {
		this.observable = observable;
		this.successKey = successKey;
		this.failureKey = failureKey;
	}

	protected Action(Observable observable, Object successKey) {
		this(observable, successKey, null);
	}

	protected abstract void doOperation(Event<T> ev);


	protected void notifyValue(Event<?> value) {
		observable.notify(successKey, value);
	}

	/**
	 * Notify this {@code Composable} that an error is being propagated through this {@code Observable}.
	 *
	 * @param error
	 * 		the error to propagate
	 */
	protected void notifyError(Throwable error) {
		observable.notify(failureKey != null ? failureKey : error.getClass(), Event.wrap(error));
	}

}
