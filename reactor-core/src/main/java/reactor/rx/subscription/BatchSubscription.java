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
package reactor.rx.subscription;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.subscription.support.WrappedSubscription;

/**
 *
 * A Subscription wrapper which request
 *
* @author Stephane Maldini
*/
public final class BatchSubscription<T> extends WrappedSubscription<T> {

	private final int         batchSize;

	public BatchSubscription(Subscription subscription, Subscriber<T> subscriber, int batchSize) {
		super(subscription, subscriber);
		this.batchSize = batchSize;
	}

	@Override
	public void request(long n) {
		if (pushSubscription != null) {
			if (n == Long.MAX_VALUE) {
				pushSubscription.request(Long.MAX_VALUE);
			} else if (pushSubscription.pendingRequestSignals() != Long.MAX_VALUE) {
				long toRequest = n*batchSize;
				toRequest = toRequest > 0 ? toRequest : Long.MAX_VALUE;
					pushSubscription.request(toRequest);
			}
		} else {
				super.request(n);
		}
	}
}
