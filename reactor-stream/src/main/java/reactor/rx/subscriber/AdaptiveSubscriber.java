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

package reactor.rx.subscriber;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class AdaptiveSubscriber<T> extends InterruptableSubscriber<T> implements ReactiveState.Bounded {

	private final Broadcaster<Long>                                           requestMapperStream;
	private final Function<Stream<Long>, ? extends Publisher<? extends Long>> requestMapper;
	private final RequestSubscriber inner = new RequestSubscriber();

	@SuppressWarnings("unused")
	private volatile long requested;
	private final AtomicLongFieldUpdater<AdaptiveSubscriber> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(AdaptiveSubscriber.class, "requested");

	@SuppressWarnings("unused")
	private volatile long outstanding;
	private final AtomicLongFieldUpdater<AdaptiveSubscriber> OUTSTANDING =
			AtomicLongFieldUpdater.newUpdater(AdaptiveSubscriber.class, "outstanding");

	public AdaptiveSubscriber(Timer timer,
			Consumer<? super T> consumer,
			Function<Stream<Long>, ? extends Publisher<? extends Long>> requestMapper) {
		super(consumer, null, null);
		this.requestMapper = requestMapper;
		this.requestMapperStream = Broadcaster.create(timer);
		this.requestMapperStream.onSubscribe(new Subscription() {
			@Override
			public void request(long n) {
				//IGNORE
			}

			@Override
			public void cancel() {
				AdaptiveSubscriber.this.cancel();
			}
		});
	}

	@Override
	protected void doSafeSubscribe(Subscription subscription) {
		Publisher<? extends Long> afterRequestStream = requestMapper.apply(requestMapperStream);
		afterRequestStream.subscribe(inner);
		requestMapperStream.onNext(0L);
	}

	@Override
	protected void doPostNext(T ev) {
		long outstanding = OUTSTANDING.incrementAndGet(this);
		if (REQUESTED.compareAndSet(this, outstanding, 0L)) {
			OUTSTANDING.addAndGet(this, -outstanding);
			requestMapperStream.onNext(outstanding);
		}
	}

	@Override
	protected void doSafeError(Throwable ev) {
		super.doSafeError(ev);
		if(!inner.done) {
			requestMapperStream.onError(ev);
		}
	}

	@Override
	protected void doSafeComplete() {
		super.doSafeComplete();
		if(!inner.done) {
			requestMapperStream.onComplete();
		}
		else{
			cancel();
		}
	}

	@Override
	public String toString() {
		return super.toString() + "{pending=" + requested + "}";
	}

	@Override
	public long getCapacity() {
		return requested;
	}

	private class RequestSubscriber implements Subscriber<Long> {

		Subscription s;
		volatile boolean done;

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Long n) {
			if(n == 0){
				return;
			}
			BackpressureUtils.checkRequest(n);
			if(BackpressureUtils.getAndAdd(REQUESTED, AdaptiveSubscriber.this, n) == 0){
				requestMore(n);
			}
		}

		@Override
		public void onError(Throwable t) {
			if(!done) {
				done = true;
				s = null;
				Exceptions.throwIfFatal(t);
				cancel();
				doSafeError(t);
			}
		}

		@Override
		public void onComplete() {
			if(!done) {
				done = true;
				s = null;
				cancel();
				doSafeComplete();
			}
		}

	}
}
