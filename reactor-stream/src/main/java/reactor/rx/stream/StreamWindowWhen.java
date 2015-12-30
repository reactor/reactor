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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.timer.Timer;
import reactor.fn.Supplier;
import reactor.rx.Stream;

/**
 * WindowAction is forwarding events on a steam until {@param boundarySupplier} returned stream emits a signal, after
 * that streams collected events further, complete it and create a fresh new stream.
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public final class StreamWindowWhen<T> extends StreamBarrier<T, Stream<T>> {

	final private Supplier<? extends Publisher<?>> boundarySupplier;
	final private Timer                            timer;

	public StreamWindowWhen(Publisher<T> source, Timer timer, Supplier<? extends Publisher<?>> boundarySupplier) {
		super(source);
		this.boundarySupplier = boundarySupplier;
		this.timer = timer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super Stream<T>> subscriber) {
		return new WindowWhenAction<>(subscriber, timer, boundarySupplier);
	}

	static final class WindowWhenAction<T> extends SubscriberWithDemand<T, Stream<T>> {

		final private Supplier<? extends Publisher<?>> boundarySupplier;
		final private Timer                            timer;

		private StreamWindow.Window<T> windowBroadcaster;

		public WindowWhenAction(Subscriber<? super Stream<T>> actual,
				Timer timer,
				Supplier<? extends Publisher<?>> boundarySupplier) {

			super(actual);
			this.boundarySupplier = boundarySupplier;
			this.timer = timer;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			subscriber.onSubscribe(this);
			boundarySupplier.get()
			                .subscribe(new Subscriber<Object>() {

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
					                s = null;
				                }
			                });
		}

		private void flush() {
			StreamWindow.Window<T> _currentWindow = windowBroadcaster;
			if (_currentWindow != null) {
				windowBroadcaster = null;
				_currentWindow.onComplete();
			}

		}

		@Override
		protected void doNext(T value) {
			if (windowBroadcaster == null) {
				subscriber.onNext(createWindowStream(value));
			}
			else {
				windowBroadcaster.onNext(value);
			}
		}

		public StreamWindow.Window<T> currentWindow() {
			return windowBroadcaster;
		}

		protected Stream<T> createWindowStream(T first) {
			StreamWindow.Window<T> action = new StreamWindow.Window<T>(timer);
			action.onNext(first);
			windowBroadcaster = action;
			return action;
		}

		@Override
		protected void checkedError(Throwable ev) {
			if (windowBroadcaster != null) {
				windowBroadcaster.onError(ev);
				windowBroadcaster = null;
			}
			subscriber.onError(ev);
		}

		@Override
		protected void checkedComplete() {
			flush();
			subscriber.onComplete();
		}

	}
}
