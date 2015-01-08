/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.action.filter;

import reactor.core.Dispatcher;
import reactor.fn.Predicate;
import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class SkipAction<T> extends Action<T, T> {

	private final Predicate<T> startPredicate;
	private final long         limit;
	private long counted = 0l;

	public SkipAction(Dispatcher dispatcher, Predicate<T> predicate, long limit) {
		super(dispatcher);
		this.startPredicate = predicate;
		this.limit = limit;
	}

	@Override
	protected void doNext(T ev) {
		if (counted == -1l || counted++ >= limit || (startPredicate != null && !startPredicate.test(ev))) {
			broadcastNext(ev);
			if(counted != 1l){
				counted = -1l;
			}
		}
	}


	@Override
	public String toString() {
		return super.toString() + "{" +
				"skip=" + limit + (startPredicate == null ?
				", with-start-predicate" : "") +
				'}';
	}
}
