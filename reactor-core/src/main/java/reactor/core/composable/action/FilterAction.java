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
package reactor.core.composable.action;

import reactor.event.dispatch.Dispatcher;
import reactor.function.Predicate;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class FilterAction<T> extends Action<T, T> {

	public static final Predicate<Boolean> simplePredicate = new Predicate<Boolean>() {
		@Override
		public boolean test(Boolean aBoolean) {
			return aBoolean;
		}
	};

	private final Predicate<T>       p;
	private final ActionProcessor<T> actionElsePublisher;

	public FilterAction(Predicate<T> p, Dispatcher dispatcher, ActionProcessor<T> actionProcessor,
	                    ActionProcessor<T> actionElsePublisher
	) {
		super(dispatcher, actionProcessor);
		this.p = p;
		this.actionElsePublisher = actionElsePublisher;
	}

	@Override
	public void doNext(T value) {
		if (p.test(value)) {
			output.onNext(value);
		} else {
			if (null != actionElsePublisher) {
				actionElsePublisher.onNext(value);
			}
			// GH-154: Verbose error level logging of every event filtered out by a Stream filter
			// Fix: ignore Predicate failures and drop values rather than notifying of errors.
			//d.accept(new IllegalArgumentException(String.format("%s failed a predicate test.", value)));
		}
		available();
	}

	@Override
	protected void doError(Throwable ev) {
		if (null != actionElsePublisher) {
			actionElsePublisher.onError(ev);
		}
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		if(null != actionElsePublisher){
			actionElsePublisher.onComplete();
		}
		super.doComplete();
	}

	public ActionProcessor<T> getElsePublisher() {
		return actionElsePublisher;
	}

}
