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
package reactor.rx.action.aggregation;

import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.rx.action.Action;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class LastAction<T> extends Action<T, T> {

	private T last;

	private final AtomicLongFieldUpdater<LastAction> COUNTED = AtomicLongFieldUpdater.newUpdater(LastAction
	  .class, "count");

	private final AtomicLongFieldUpdater<LastAction> OUTSTANDING = AtomicLongFieldUpdater.newUpdater(LastAction
			.class, "outstanding");

	private volatile long count;
	private volatile long outstanding;

	@Override
	protected void doNext(T value) {
		last = value;
		if(outstanding != Long.MAX_VALUE && COUNTED.decrementAndGet(this) == 0L){
			super.requestMore(count = outstanding);
		}
	}

	@Override
	public void requestMore(long n) {
		BackpressureUtils.checkRequest(n);
		long before = BackpressureUtils.getAndAdd(OUTSTANDING, this, n);
		BackpressureUtils.getAndAdd(COUNTED, this, n);
		if(before == 0) {
			super.requestMore(n);
		}

	}

	@Override
	protected void doComplete() {
		if (last != null) {
			broadcastNext(last);
		}

		super.doComplete();
	}
}
