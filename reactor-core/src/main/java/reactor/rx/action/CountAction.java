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

import reactor.event.dispatch.Dispatcher;
import reactor.rx.Stream;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class CountAction<T> extends Action<T, T> {

	private final AtomicLong counter = new AtomicLong(0l);
	private final Action<T, Long> stream;

	public CountAction(Dispatcher dispatcher) {
		super(dispatcher);
		this.stream = new Action<T,Long>(dispatcher);
	}

	@Override
	protected void doNext(T value) {
		counter.getAndIncrement();
		available();
	}

	public Stream<Long> valueStream() {
		return connect(stream);
	}

	@Override
	protected void doFlush() {
		stream.broadcastNext(counter.get());
		counter.set(0l);
		valueStream().broadcastFlush();
		super.doFlush();
	}

}
