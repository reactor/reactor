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

import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.function.Consumer;
import reactor.timer.Timer;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class TakeUntilTimeout<T> extends Action<T, T> {

	private final long     time;
	private final TimeUnit unit;
	private final Timer    timer;


	public TakeUntilTimeout(Dispatcher dispatcher, long time, TimeUnit unit, Timer timer) {
		super(dispatcher);
		this.unit = unit;
		this.timer = timer;
		this.time = time;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		timer.submit(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				cancel();
				dispatch(new Consumer<Void>() {
					@Override
					public void accept(Void aVoid) {
						broadcastComplete();
					}
				});

			}
		}, time, unit);
	}

	@Override
	public String toString() {
		return super.toString() + "{" +
				"time=" + time +
				"unit=" + unit +
				'}';
	}
}
