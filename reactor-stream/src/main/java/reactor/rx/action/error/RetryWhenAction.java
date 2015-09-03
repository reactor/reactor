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
package reactor.rx.action.error;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.support.Bounded;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.subscription.PushSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class RetryWhenAction<T> extends Action<T, T> {

	private final Broadcaster<Throwable> retryStream;
	private final Publisher<? extends T> rootPublisher;

	public RetryWhenAction(
	  Timer timer, Function<? super Stream<? extends Throwable>, ? extends Publisher<?>> predicate, Publisher<?
	  extends
	  T> rootPublisher) {
		this.retryStream = Broadcaster.create(timer);
		this.rootPublisher = rootPublisher != null ? Publishers.trampoline(rootPublisher) : null;
		Publisher<?> afterRetryPublisher = predicate.apply(retryStream);
		afterRetryPublisher.subscribe(new RestartSubscriber());
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	protected void doComplete() {
		retryStream.onComplete();
		super.doComplete();
	}

	protected void doRetry() {

		long pendingRequests = Long.MAX_VALUE;
		if (rootPublisher != null) {
			PushSubscription<T> upstream = upstreamSubscription;
			if (upstream == null) {
				rootPublisher.subscribe(RetryWhenAction.this);
				upstream = upstreamSubscription;
			} else {
				pendingRequests = upstream.pendingRequestSignals();
			}
			if (upstream != null) {
				upstream.request(pendingRequests != Long.MAX_VALUE ? pendingRequests + 1 : pendingRequests);
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onError(Throwable cause) {
		cancel();
		retryStream.onNext(cause);
	}

	public Broadcaster<Throwable> retryStream() {
		return retryStream;
	}

	private class RestartSubscriber implements Subscriber<Object>, Bounded {
		Subscription s;

		@Override
		public boolean isExposedToOverflow(Bounded upstream) {
			return RetryWhenAction.this.isExposedToOverflow(upstream);
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
		public void onNext(Object o) {
			//s.cancel();
			//publisher.subscribe(this);
			doRetry();
			if (s != null) {
				s.request(1l);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (s != null) {
				s.cancel();
			}
			RetryWhenAction.this.onError(t);
		}

		@Override
		public void onComplete() {
			RetryWhenAction.this.onComplete();
		}
	}
}
