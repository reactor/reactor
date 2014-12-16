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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * WindowAction is forwarding events on a steam until {@param boundarySupplier} returned stream emits a signal,
 * after that streams collected events further, complete it and create a fresh new stream.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class WindowWhenAction<T> extends Action<T, Stream<T>> {

	private ReactiveSubscription<T> currentWindow;

	final Supplier<? extends Publisher<?>> boundarySupplier;

	public WindowWhenAction(Dispatcher dispatcher, Supplier<? extends Publisher<?>> boundarySupplier) {
		super(dispatcher);
		this.boundarySupplier = boundarySupplier;
	}



	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
		final Consumer<Void> consumerFlush = new Consumer<Void>() {
			@Override
			public void accept(Void o) {
				flush();
			}
		};

		boundarySupplier.get().subscribe(new Subscriber<Object>() {

			Subscription s;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1);
			}

			@Override
			public void onNext(Object o) {
				dispatch(consumerFlush);
				if (s != null) {
					s.request(1);
				}
			}

			@Override
			public void onError(Throwable t) {
				if (s != null) {
					s.cancel();
				}
				WindowWhenAction.this.onError(t);
			}

			@Override
			public void onComplete() {
				if (s != null) {
					s.cancel();
				}
				WindowWhenAction.this.onComplete();
			}
		});
	}

	private void flush() {
		ReactiveSubscription<T> _currentWindow = currentWindow;
		if (_currentWindow != null) {
			currentWindow = null;
			_currentWindow.onComplete();
		}


		broadcastNext(createWindowStream());

	}

	@Override
	protected void doNext(T value) {
		if(currentWindow == null) {
			broadcastNext(createWindowStream());
		}
		currentWindow.onNext(value);
	}

	public ReactiveSubscription<T> currentWindow() {
		return currentWindow;
	}

	protected Stream<T> createWindowStream() {
		Action<T,T> action = Action.<T>broadcast(dispatcher, capacity).env(environment);
		ReactiveSubscription<T> _currentWindow = new ReactiveSubscription<T>(null, action);
		currentWindow = _currentWindow;
		action.onSubscribe(_currentWindow);
		return action;
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		if (currentWindow != null) {
			currentWindow.onError(ev);
			currentWindow = null;
		}
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		if (currentWindow != null) {
			currentWindow.onComplete();
			currentWindow = null;
		}
	}


}
