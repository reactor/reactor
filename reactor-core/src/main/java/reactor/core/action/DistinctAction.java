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
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.timer.Timer;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class DistinctAction<T> extends Action<T> {

	private T lastData;

	public DistinctAction(Observable d, Object successKey, Object failureKey) {
		super(d, successKey, failureKey);
	}

	@Override
	protected void doAccept(Event<T> ev) {
		final T currentData = ev.getData();
		if(currentData == null || !currentData.equals(lastData)){
			lastData = currentData;
			notifyValue(ev);
		}
	}
}
