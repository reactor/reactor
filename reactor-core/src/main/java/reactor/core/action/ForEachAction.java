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
import reactor.event.selector.Selector;

/**
 * @author Stephane Maldini
 */
public class ForEachAction<T> extends Action<Iterable<T>> {

	final private Iterable<T> values;

	public ForEachAction(Observable d, Object successKey, Object failureKey) {
		this(null, d, successKey, failureKey);
	}

	public ForEachAction(Iterable<T> values, Observable d, Object successKey, Object failureKey) {
		super(d, successKey, failureKey);
		this.values = values;
	}

	public ForEachAction<T> attach(Selector flushKey) {
		getObservable().on(flushKey, new ForEachFlushAction());
		return this;
	}

	@Override
	public void doAccept(Event<Iterable<T>> value) {
		if(value.getData() != null) {
			for(T val : value.getData()) {
				notifyValue(value.copy(val));
			}
		}
	}

	private class ForEachFlushAction extends Action<Void> {
		public ForEachFlushAction() {
			super(ForEachAction.this.getObservable(), ForEachAction.this.getSuccessKey());
		}

		@Override
		public void doAccept(Event<Void> ev) {
			if(values != null) { ForEachAction.this.doAccept(ev.copy(values)); }
		}
	}
}
