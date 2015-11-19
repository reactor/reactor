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
package reactor.rx.action.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.fn.Supplier;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class BufferWhenOperator<T> implements Publishers.Operator<T, List<T>> {

	private final Supplier<? extends Publisher<?>> boundarySupplier;

	public BufferWhenOperator(Supplier<? extends Publisher<?>> boundarySupplier) {
		this.boundarySupplier = boundarySupplier;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super List<T>> subscriber) {
		return new BufferWhenAction<>(subscriber, boundarySupplier);
	}

	static final class BufferWhenAction<T> extends SubscriberWithDemand<T, List<T>> {

		private final List<T> values = new ArrayList<T>();
		private final Supplier<? extends Publisher<?>> boundarySupplier;

		public BufferWhenAction(Subscriber<? super List<T>> actual, Supplier<? extends Publisher<?>> boundarySupplier) {
			super(actual);
			this.boundarySupplier = boundarySupplier;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			subscriber.onSubscribe(this);

			boundarySupplier.get().subscribe(new Subscriber<Object>() {

				Subscription s;

				@Override
				public void onSubscribe(Subscription s) {
					this.s = s;
					s.request(1);
				}

				@Override
				public void onNext(Object o) {
					flush();
					if (s != null) {
						s.request(1);
					}
				}

				@Override
				public void onError(Throwable t) {
					cancel();
					subscriber.onError(t);
				}

				@Override
				public void onComplete() {
					cancel();
				}
			});
		}

		private void flush() {
			List<T> toSend;
			synchronized (values) {
				if (values.isEmpty()) {
					return;
				}
				toSend = new ArrayList<T>(values);
				values.clear();
			}

			subscriber.onNext(toSend);
		}

		@Override
		protected void doError(Throwable ev) {
			synchronized (values) {
				values.clear();
			}
			subscriber.onError(ev);
		}

		@Override
		protected void checkedComplete() {
			boolean last;
			synchronized (values){
				last = values.isEmpty();
			}
			if(!last){
				subscriber.onNext(values);
			}
			subscriber.onComplete();
		}

		@Override
		protected void doNext(T value) {
			values.add(value);
		}


	}


}
