/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.transformation;

import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class DefaultIfEmptyAction<T> extends Action<T, T> {

	private final T defaultValue;
	private boolean hasValues = false;

	public DefaultIfEmptyAction(T defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	protected void doNext(T ev) {
		hasValues = true;
		broadcastNext(ev);
	}

	@Override
	protected void doComplete() {
		if(!hasValues){
			broadcastNext(defaultValue);
		}
		broadcastComplete();
	}

}
