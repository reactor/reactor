/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Supplier;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.broadcast.BehaviorBroadcaster;
import reactor.rx.broadcast.Broadcaster;

/**
 * WindowAction is forwarding events on a steam until {@param boundarySupplier} returned stream emits a signal,
 * after that streams collected events further, complete it and create a fresh new stream.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class WindowWhenAction<T> extends Action<T, Stream<T>> {

	final private Supplier<? extends Publisher<?>> boundarySupplier;
	final private Timer                            timer;

	private Broadcaster<T> windowBroadcaster;

	public WindowWhenAction(Timer timer, Supplier<? extends Publisher<?>>
	  boundarySupplier) {
		this.boundarySupplier = boundarySupplier;
		this.timer = timer;
	}


	@Override
	protected void doOnSubscribe(Subscription subscription) {
		super.doOnSubscribe(subscription);
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
		Broadcaster<T> _currentWindow = windowBroadcaster;
		if (_currentWindow != null) {
			windowBroadcaster = null;
			_currentWindow.onComplete();
		}

	}

	@Override
	protected void doNext(T value) {
		if (windowBroadcaster == null) {
			broadcastNext(createWindowStream(value));
		} else {
			windowBroadcaster.onNext(value);
		}
	}

	public Broadcaster<T> currentWindow() {
		return windowBroadcaster;
	}

	protected Stream<T> createWindowStream(T first) {
		Broadcaster<T> action = BehaviorBroadcaster.first(first, timer);
		windowBroadcaster = action;
		return action;
	}

	@Override
	protected void doError(Throwable ev) {
		if (windowBroadcaster != null) {
			windowBroadcaster.onError(ev);
			windowBroadcaster = null;
		}
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		if (windowBroadcaster != null) {
			windowBroadcaster.onComplete();
			windowBroadcaster = null;
		}
		super.doComplete();
	}

	@Override
	public final Timer getTimer() {
		return timer;
	}
}
