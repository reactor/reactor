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

package reactor.core.publisher.operator;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.Assert;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Function;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class OnErrorResumeOperator<T> implements Function<Subscriber<? super T>, Subscriber<? super T>>,
                                                          ReactiveState.Factory {

	private final Function<Throwable, ? extends Publisher<? extends T>> fallbackSelector;

	public OnErrorResumeOperator(Function<Throwable, ? extends Publisher<? extends T>> fallbackSelector) {
		this.fallbackSelector = fallbackSelector;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new ErrorSelectBarrier<>(subscriber, fallbackSelector);
	}

	static final class ErrorSelectBarrier<T> extends SubscriberBarrier<T, T> implements ReactiveState.Named {

		private final  Function<Throwable, ? extends Publisher<? extends T>> fallbackSelector;

		public ErrorSelectBarrier(
				Subscriber<? super T> actual,
				Function<Throwable, ? extends Publisher<? extends T>> fallbackSelector) {
			super(actual);
			Assert.notNull(fallbackSelector, "Fallback Selector function cannot be null.");
			this.fallbackSelector = fallbackSelector;
		}

		@Override
		public String getName() {
			return ReactiveStateUtils.getName(fallbackSelector);
		}

		@Override
		protected void doError(Throwable throwable) {
			super.doError(throwable);
			Publisher<? extends T> fallback = fallbackSelector.apply(throwable);
		}
	}

}
