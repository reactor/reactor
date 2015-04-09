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
package reactor.rx.action.control;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.support.NonBlocking;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ThrottleRequestWhenAction<T> extends Action<T, T> {

	private final Broadcaster<Long> throttleStream;

	public ThrottleRequestWhenAction(Dispatcher dispatcher,
	                                 Function<? super Stream<? extends Long>, ? extends Publisher<? extends Long>>
			                                 predicate) {
		this.throttleStream = Broadcaster.create(null, dispatcher);
		Publisher<? extends Long> afterRequestStream = predicate.apply(throttleStream);
		afterRequestStream.subscribe(new ThrottleSubscriber());
	}

	@Override
	public void requestMore(long elements) {
		throttleStream.onNext(elements);
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	public void onComplete() {
		try {
			throttleStream.onComplete();
			doShutdown();
		}catch(Exception e){
			doError(e);
		}
	}

	@Override
	public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
		return true;
	}

	protected void doRequest(final long requested) {
		throttleStream.getDispatcher().dispatch(requested, new Consumer<Long>() {
			@Override
			public void accept(Long o) {
				if (upstreamSubscription != null) {
					upstreamSubscription.request(o);
				}
			}
		}, null);
	}

	private class ThrottleSubscriber implements Subscriber<Long>, NonBlocking {
		Subscription s;

		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return false;
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			s.request(1l);
		}

		@Override
		public void onNext(Long o) {
			//s.cancel();
			//publisher.subscribe(this);
			if (o > 0) {
				doRequest(o);
			}
			s.request(1l);
		}

		@Override
		public void onError(Throwable t) {
			s.cancel();
			ThrottleRequestWhenAction.this.doError(t);
		}

		@Override
		public void onComplete() {
			s.cancel();
			ThrottleRequestWhenAction.this.doComplete();
		}
	}
}
