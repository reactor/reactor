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
package reactor.rx.stream;

import org.reactivestreams.*;

import reactor.core.subscriber.SubscriberDeferredScalar;
import reactor.core.support.BackpressureUtils;


/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoIsEmpty<T> extends reactor.Mono.MonoBarrier<T, Boolean> {

	public MonoIsEmpty(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super Boolean> s) {
		source.subscribe(new MonoIsEmptySubscriber<>(s));
	}

	static final class MonoIsEmptySubscriber<T> extends SubscriberDeferredScalar<T, Boolean>
	implements Upstream {
		Subscription s;

		public MonoIsEmptySubscriber(Subscriber<? super Boolean> actual) {
			super(actual);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				subscriber.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			s.cancel();

			set(false);
		}

		@Override
		public void onComplete() {
			set(true);
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
}
