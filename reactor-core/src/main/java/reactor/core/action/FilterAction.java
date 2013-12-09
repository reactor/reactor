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
package reactor.core.action;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.function.Predicate;

/**
 * @author Stephane Maldini
 */
public class FilterAction<T> extends Action<T> {
	private final Predicate<T> p;
	private final Observable   elseObservable;
	private final Object       elseSuccess;

	public FilterAction(Predicate<T> p, Observable d, Object successKey, Object failureKey) {
		this(p, d, successKey, failureKey, null, null);
	}

	public FilterAction(Predicate<T> p, Observable d, Object successKey, Object failureKey,
	                    Observable elseObservable, Object elseSuccess
	) {
		super(d, successKey, failureKey);
		this.p = p;
		this.elseSuccess = elseSuccess;
		this.elseObservable = elseObservable;
	}

	@Override
	public void doAccept(Event<T> value) {
		boolean b = p.test(value.getData());
		if(b) {
			notifyValue(value);
		} else {
			if(null != elseObservable) {
				elseObservable.notify(elseSuccess, value);
			}
			// GH-154: Verbose error level logging of every event filtered out by a Stream filter
			// Fix: ignore Predicate failures and drop values rather than notifying of errors.
			//d.accept(new IllegalArgumentException(String.format("%s failed a predicate test.", value)));
		}
	}

	public Object getElseSuccess() {
		return elseSuccess;
	}

	public Observable getElseObservable() {
		return elseObservable;
	}

}
