/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.ReactiveState;
import reactor.fn.Function;

/**
 * Ignore onNext signals and therefore only pass request, cancel upstream and complete, error downstream
 * @author Stephane Maldini
 * @since 2.5
 */
public final class PublisherIgnoreElements<IN> extends PublisherFactory.PublisherBarrier<IN, Void> {

	public PublisherIgnoreElements(Publisher<IN> source) {
		super(source);
	}

	@Override
	public Subscriber<? super IN> apply(Subscriber<? super Void> subscriber) {
		return new CompletableBarrier<>(subscriber);
	}

	private static class CompletableBarrier<IN> extends SubscriberBarrier<IN, Void> {

		public CompletableBarrier(Subscriber<? super Void> subscriber) {
			super(subscriber);
		}

		@Override
		protected void doNext(IN in) {
		}

	}

}
