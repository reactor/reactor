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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.StreamSubscription;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanInSubscription<O> extends StreamSubscription<O> {
	final Action<?, O>            publisher;
	final Map<Long, Subscription> subs;
	final AtomicLong              pendingSubscriptionAvailable;

	public FanInSubscription(Action<?, O> publisher, Subscriber<O> subscriber) {
		this(publisher, subscriber, new HashMap<Long, Subscription>(8));
	}

	public FanInSubscription(Action<?, O> publisher, Subscriber<O> subscriber,
	                         Map<Long, Subscription> subs) {
		super(publisher, subscriber);
		this.publisher = publisher;
		this.subs = subs;
		this.pendingSubscriptionAvailable = new AtomicLong();
	}

	@Override
	public void request(final int elements) {
		final int parallel = subs.size();
		super.request(elements);

		if (parallel > 0) {
			int batchSize = elements / parallel;
			for (Subscription sub : subs.values()) {
				sub.request(batchSize);
			}

		} else if (publisher != null && parallel == 0) {
			publisher.requestUpstream(capacity, buffer.isComplete(), elements);
		}
	}

	@Override
	public void cancel() {
		for (Subscription subscription : subs.values()) {
			if (subscription != null) {
				subscription.cancel();
			}
		}
		super.cancel();
	}
}
