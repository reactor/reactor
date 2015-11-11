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
package reactor.rx.subscription;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.rx.subscription.support.WrappedSubscription;

/**
 * A Subscription wrapper which request
 *
 * @author Stephane Maldini
 */
public final class BatchSubscription<T> extends WrappedSubscription<T> {

	private final int batchSize;

	public BatchSubscription(Subscription subscription, Subscriber<T> subscriber, int batchSize) {
		super(subscription, subscriber);
		this.batchSize = batchSize;
	}

	@Override
	public void request(long n) {
		n = batchSize == Integer.MAX_VALUE ? Long.MAX_VALUE : n;
		n = n == Long.MAX_VALUE ? n : BackpressureUtils.multiplyOrLongMax(n, batchSize);
		if (pushSubscription != null) {
			pushSubscription.request(n);
		} else {
			super.request(n);
		}
	}
}
