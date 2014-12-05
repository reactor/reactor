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
package reactor.rx.action;

import reactor.core.Dispatcher;
import reactor.function.Predicate;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
final public class IgnoreErrorAction<T> extends Action<T, T> {

	private final Predicate<? super Throwable>   ignorePredicate;

	public IgnoreErrorAction(Dispatcher dispatcher, Predicate<? super Throwable> ignorePredicate) {
		super(dispatcher);
		this.ignorePredicate = ignorePredicate;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doError(Throwable cause) {
		if (!ignorePredicate.test(cause)) {
			super.doError(cause);
		}
	}

	@Override
	public String toString() {
		return super.toString()+"{" +
				"ignorePredicate=" + ignorePredicate+
				'}';
	}
}
