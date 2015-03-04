/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
import reactor.rx.action.Signal;

/**
 * @author Stephane Maldini
 */
public class DematerializeAction<T> extends Action<Signal<T>, T> {

	@Override
	protected void doNext(Signal<T> ev) {
		if(!ev.isOnSubscribe()){
			if(ev.isOnNext()){
				broadcastNext(ev.get());
			}else if(ev.isOnComplete()){
				broadcastComplete();
			}else{
				broadcastError(ev.getThrowable());
			}
		}
	}
}
