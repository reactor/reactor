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

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.StreamSubscription;

/**
* @author Stephane Maldini
* @since 2.0
*/
class CompositeSubscription<O> extends StreamSubscription<O> {
	private final Action<?, O>             publisher;
	private final CompositeSubscription<O> outerSubscription;

	protected final MultiReaderFastList<Subscription> subs;

	public CompositeSubscription(Action<?, O> publisher, Subscriber<O> subscriber,
	                             MultiReaderFastList<Subscription> subs, CompositeSubscription<O> outerSubscription) {
		super(publisher, subscriber);
		this.outerSubscription = outerSubscription;
		this.publisher = publisher;
		this.subs = subs;
	}

	@Override
	public void request(final int elements) {
		int parallel = subs.size();
		super.request(elements);

		if(outerSubscription != null){
			outerSubscription.request(elements);
		}

		if (parallel > 0) {
			subs.withReadLockAndDelegate(new Procedure<MutableList<Subscription>>() {
				@Override
				public void value(MutableList<Subscription> subscriptions) {
					subscriptions.forEach(new Procedure<Subscription>() {
						@Override
						public void value(Subscription subscription) {
							subscription.request(elements);
						}
					});
				}
			});
		}
		if (publisher != null) {
			publisher.requestUpstream(capacity, buffer.isComplete(), elements);
		}
	}

	@Override
	public void cancel() {
		for (Subscription subscription : subs) {
			if (subscription != null) {
				subscription.cancel();
			}
		}
		super.cancel();
	}
}
