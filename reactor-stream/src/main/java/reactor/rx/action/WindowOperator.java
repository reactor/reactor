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

package reactor.rx.action;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Processors;
import reactor.core.processor.BaseProcessor;
import reactor.core.timer.Timer;
import reactor.rx.Stream;

/**
 * WindowAction is forwarding events on a steam until {@param backlog} is reached, after that streams collected events
 * further, complete it and create a fresh new stream.
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public class WindowOperator<T> extends BatchOperator<T, Stream<T>> {

	protected final Timer timer;

	public WindowOperator(Timer timer, int backlog) {
		super(backlog, true, true, true);
		this.timer = timer;
	}

	public WindowOperator(int backlog, long timespan, TimeUnit unit, Timer timer) {
		super(backlog, true, true, true, timespan, unit, timer);
		this.timer = timer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super Stream<T>> subscriber) {
		return new WindowAction<>(prepareSub(subscriber), batchSize, timespan, unit, timer);
	}

	final static class Window<T> extends Stream<T> implements Subscriber<T>, Subscription{

		final protected BaseProcessor<T, T> processor;
		final protected Timer timer;

		protected int count = 0;

		public Window(Timer timer) {
			this(timer, BaseProcessor.SMALL_BUFFER_SIZE);
		}

		public Window(Timer timer, int size) {
			this.processor = Processors.emitter(size);
			this.processor.onSubscribe(this);
			this.timer = timer;
		}

		@Override
		public Timer getTimer() {
			return timer;
		}

		@Override
		public long getCapacity() {
			return processor.getCapacity();
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.cancel();
		}

		@Override
		public void onNext(T t) {
			count++;
			processor.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			processor.onError(t);
		}

		@Override
		public void onComplete() {
			processor.onComplete();
		}

		@Override
		public void subscribe(Subscriber<? super T> s) {
			processor.subscribe(s);
		}

		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}

		@Override
		public String toString() {
			return super.toString();
		}
	}

	final static class WindowAction<T> extends BatchOperator.BatchAction<T, Stream<T>> {

		private final Timer timer;

		private Window<T> currentWindow;

		public WindowAction(Subscriber<? super Stream<T>> actual,
				int backlog,
				long timespan,
				TimeUnit unit,
				Timer timer) {

			super(actual, backlog, true, true, true, timespan, unit, timer);
			this.timer = timer;
		}

		public Window<T> currentWindow() {
			return currentWindow;
		}

		protected Stream<T> createWindowStream() {
			Window<T> _currentWindow = new Window<T>(timer);
			_currentWindow.onSubscribe(new Subscription(){

				@Override
				public void cancel() {
					currentWindow = null;
				}

				@Override
				public void request(long n) {

				}
			});
			currentWindow = _currentWindow;
			return _currentWindow;
		}

		@Override
		protected void doError(Throwable ev) {
			if (currentWindow != null) {
				currentWindow.onError(ev);
			}
			super.doError(ev);
		}

		@Override
		protected void doComplete() {
			if (currentWindow != null) {
				currentWindow.onComplete();
				currentWindow = null;
			}
			super.doComplete();
		}

		@Override
		protected void firstCallback(T event) {
			subscriber.onNext(createWindowStream());
		}

		@Override
		protected void nextCallback(T event) {
			if (currentWindow != null) {
				currentWindow.onNext(event);
			}
		}

		@Override
		protected void flushCallback(T event) {
			if (currentWindow != null) {
				currentWindow.onComplete();
				currentWindow = null;
			}
		}

	}


}
