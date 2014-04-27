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

import reactor.event.dispatch.Dispatcher;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class MergeAction<O> extends Action<O, O> {

	final AtomicInteger runningComposables;
	final Action<O, ?>  delegateAction;

	private final static Pipeline[] EMPTY_PIPELINE = new Pipeline[0];

	@SuppressWarnings("unchecked")
	public MergeAction(Dispatcher dispatcher) {
		this(dispatcher, Integer.MAX_VALUE, null, EMPTY_PIPELINE);
	}

	public MergeAction(Dispatcher dispatcher, Pipeline<O>... composables) {
		this(dispatcher, composables.length + 1, null, composables);
	}

	public MergeAction(Dispatcher dispatcher, int length, Pipeline<O>... composables) {
		this(dispatcher, length, null, composables);
	}

	public MergeAction(Dispatcher dispatcher, int length, final Action<O, ?> delegateAction,
	                   Pipeline<O>... composables) {
		super(dispatcher);
		this.delegateAction = delegateAction;

		if (composables != null && composables.length > 0) {
			this.runningComposables = new AtomicInteger(length);
			for (Pipeline<O> composable : composables) {
				composable.connect(new Action<O, O>(dispatcher) {
					@Override
					protected void doFlush() {
						MergeAction.this.doFlush();
						if (delegateAction != null) {
							delegateAction.onFlush();
						}
					}

					@Override
					protected void doComplete() {
						MergeAction.this.doComplete();
					}

					@Override
					protected void doNext(O ev) {
						MergeAction.this.doNext(ev);
						if (delegateAction != null) {
							delegateAction.onNext(ev);
						}
					}

					@Override
					protected void doError(Throwable ev) {
						MergeAction.this.doError(ev);
						if (delegateAction != null) {
							delegateAction.onError(ev);
						}
					}
				});
			}
		} else {
			this.runningComposables = new AtomicInteger(0);
		}
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	@Override
	protected void doComplete() {
		if (runningComposables.decrementAndGet() == 0) {
			broadcastComplete();
			if (delegateAction != null) {
				delegateAction.onComplete();
			}
		}
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables +
				'}';
	}
}
