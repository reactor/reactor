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
import reactor.core.support.ReactiveState;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class DecoratingStream<T> extends Stream<T> implements ReactiveState.Upstream {

	private final Publisher<T> publisher;

	public DecoratingStream(Publisher<T> publisher) {
		this.publisher = publisher;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		publisher.subscribe(s);
	}

	@Override
	public Object upstream() {
		return publisher;
	}

	@Override
	public long getCapacity() {
		return ReactiveState.Bounded.class.isAssignableFrom(publisher.getClass()) ? ((ReactiveState.Bounded) publisher).getCapacity() : Long.MAX_VALUE;
	}

	@Override
	public Timer getTimer() {
		return Stream.class.isAssignableFrom(publisher.getClass()) ? ((Stream) publisher).getTimer() : super.getTimer();
	}


	@Override
	public String toString() {
		return "Stream{" +
				"publisher=" + publisher +
				'}';
	}
}
