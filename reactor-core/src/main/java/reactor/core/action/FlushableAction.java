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

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class FlushableAction extends Action<Void> {

	private final Flushable<?> flushable;

	public FlushableAction(Flushable<?> flushable, Observable d, Object failureKey) {
		super(d, null, failureKey);
		this.flushable = flushable;
	}

	@Override
	public void doAccept(Event<Void> value) {
		flushable.flush();
	}

	@Override
	public String toString() {
		return " "+flushable.getClass().getSimpleName().replaceAll("Action","")+"["+flushable.toString()+"]";
	}
}
