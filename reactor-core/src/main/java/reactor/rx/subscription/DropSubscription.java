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
package reactor.rx.subscription;

import org.reactivestreams.Subscriber;
import reactor.rx.Stream;
import reactor.rx.action.Action;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Relationship between a Stream (Publisher) and a Subscriber.
 * <p>
 * - If no capacity (no previous request or capacity drained), drop data
 * - If capacity (previous request and capacity remaining), call subscriber onNext
 * <p>
 * Each next signal will decrement the capacity by 1.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class DropSubscription<O> extends PushSubscription<O> {
	protected final AtomicLong capacity;

	public DropSubscription(Stream<O> publisher, Subscriber<? super O> subscriber) {
		super(publisher, subscriber);
		this.capacity = new AtomicLong();
	}

	@Override
	public void request(long elements) {
		Action.checkRequest(elements);
		capacity.addAndGet(elements);
	}

	@Override
	public void cancel() {
		super.cancel();
		capacity.set(0l);
	}

	@Override
	public void onNext(O ev) {
		if (capacity.getAndDecrement() > 0) {
			subscriber.onNext(ev);
		} else if (capacity.incrementAndGet() > 0) {
			onNext(ev);
		}
	}

	@Override
	public String toString() {
		return "{" +
				"capacity=" + capacity +
				'}';
	}
}
