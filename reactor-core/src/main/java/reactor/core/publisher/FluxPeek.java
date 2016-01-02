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
import org.reactivestreams.Subscription;
import reactor.fn.Consumer;
import reactor.fn.LongConsumer;

/**
 * Peek into the lifecycle events and signals of a sequence.
 * <p>
 * <p>
 * The callbacks are all optional.
 * <p>
 * <p>
 * Crashes by the lambdas are ignored.
 *
 * @param <T> the value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 *
 * @since 2.5
 */
public final class FluxPeek<T> extends reactor.Flux.FluxBarrier<T, T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final Runnable onAfterTerminateCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	public FluxPeek(Publisher<? extends T> source,
			Consumer<? super Subscription> onSubscribeCall,
			Consumer<? super T> onNextCall,
			Consumer<? super Throwable> onErrorCall,
			Runnable onCompleteCall,
			Runnable onAfterTerminateCall,
			LongConsumer onRequestCall,
			Runnable onCancelCall) {
		super(source);
		this.onSubscribeCall = onSubscribeCall;
		this.onNextCall = onNextCall;
		this.onErrorCall = onErrorCall;
		this.onCompleteCall = onCompleteCall;
		this.onAfterTerminateCall = onAfterTerminateCall;
		this.onRequestCall = onRequestCall;
		this.onCancelCall = onCancelCall;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new FluxPeekSubscriber<>(s, this));
	}

	static final class FluxPeekSubscriber<T> implements Subscriber<T>, Subscription {

		final Subscriber<? super T> actual;

		final FluxPeek<T> parent;

		Subscription s;

		public FluxPeekSubscriber(Subscriber<? super T> actual, FluxPeek<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			try {
				parent.onRequestCall.accept(n);
			}
			catch (Throwable e) {
				// FIXME nowhere to go
			}
			s.request(n);
		}

		@Override
		public void cancel() {
			try {
				parent.onCancelCall.run();
			}
			catch (Throwable e) {
				// FIXME nowhere to go
			}
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			try {
				parent.onSubscribeCall.accept(s);
			}
			catch (Throwable e) {
				// FIXME nowhere to go
			}
			this.s = s;
			actual.onSubscribe(this);
		}

		@Override
		public void onNext(T t) {
			try {
				parent.onNextCall.accept(t);
			}
			catch (Throwable e) {
				// FIXME nowhere to go
			}
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			try {
				parent.onErrorCall.accept(t);
			}
			catch (Throwable e) {
				// FIXME nowhere to go
			}

			actual.onError(t);

			try {
				parent.onAfterTerminateCall.run();
			}
			catch (Throwable e) {
				// FIXME nowhere to go
			}
		}

		@Override
		public void onComplete() {
			try {
				parent.onCompleteCall.run();
			}
			catch (Throwable e) {
				// FIXME nowhere to go
			}

			actual.onComplete();

			try {
				parent.onAfterTerminateCall.run();
			}
			catch (Throwable e) {
				// FIXME nowhere to go
			}
		}

	}
}
