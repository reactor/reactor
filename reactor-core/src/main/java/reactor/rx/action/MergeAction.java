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
	private final static Pipeline[] EMPTY_PIPELINE = new Pipeline[0];

	@SuppressWarnings("unchecked")
	public MergeAction(Dispatcher dispatcher) {
		this(dispatcher, 0, EMPTY_PIPELINE);
	}

	@SuppressWarnings("unchecked")
	public MergeAction(Dispatcher dispatcher, Pipeline<O>... composables) {
		this(dispatcher, composables.length + 1, composables);
	}

	public MergeAction(Dispatcher dispatcher, int length, Pipeline<O>... composables) {
		super(dispatcher);
		if (composables != null && composables.length > 0) {
			this.runningComposables = new AtomicInteger(length);
			for (Pipeline<O> composable : composables) {
				composable.connect(new Action<O, Object>(dispatcher) {
					@Override
					protected void doFlush() {
						MergeAction.this.onFlush();
					}

					@Override
					protected void doComplete() {
						MergeAction.this.onComplete();
					}

					@Override
					protected void doNext(O ev) {
						MergeAction.this.onNext(ev);
					}

					@Override
					protected void doError(Throwable ev) {
						MergeAction.this.doError(ev);
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
		super.doNext(ev);
	}

	@Override
	protected void doComplete() {
		if (runningComposables.decrementAndGet() == 0) {
			super.doComplete();
		}
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables +
				'}';
	}
}
