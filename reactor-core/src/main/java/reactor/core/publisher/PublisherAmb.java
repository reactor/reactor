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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.SignalType;
import reactor.core.support.internal.PlatformDependent;

/**
 * An amb operator to select the first emitting sequence
 * <p>
 * {@see http://github.com/reactive-streams-commons}
 *
 * @since 2.1
 */
public final class PublisherAmb<T> implements Publisher<T>, ReactiveState.Factory, ReactiveState.LinkedUpstreams {

	final Publisher[] sources;

	public PublisherAmb(final Publisher[] sources) {
		this.sources = sources;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s) {
		if (s == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}
		try {
			if (sources == null || sources.length == 0) {
				s.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
				s.onComplete();
				return;
			}

			if (sources.length == 1) {
				sources[1].subscribe(s);
				return;
			}

			AmbBarrier<T> barrier = new AmbBarrier<>(s, sources.length);
			barrier.subscribe(sources);
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			s.onError(t);
		}
	}

	@Override
	public Iterator<?> upstreams() {
		return Arrays.asList(sources)
		             .iterator();
	}

	@Override
	public long upstreamsCount() {
		return sources != null ? sources.length : 0;
	}

	static final class AmbBarrier<T> implements Subscription {

		final Subscriber<? super T>   actual;
		final AmbInnerSubscriber<T>[] subscribers;

		volatile int winner;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<AmbBarrier> WINNER =
				AtomicIntegerFieldUpdater.newUpdater(AmbBarrier.class, "winner");

		@SuppressWarnings("unchecked")
		public AmbBarrier(Subscriber<? super T> actual, int count) {
			this.actual = actual;
			this.subscribers = new AmbInnerSubscriber[count];
			WINNER.lazySet(this, 0); // release the contents of 'as'
		}

		public void subscribe(Publisher<? extends T>[] sources) {
			AmbInnerSubscriber<T>[] as = subscribers;
			int len = as.length;
			for (int i = 0; i < len; i++) {
				as[i] = new AmbInnerSubscriber<>(this, i + 1, actual);
			}
			actual.onSubscribe(this);

			for (int i = 0; i < len; i++) {
				if (winner != 0) {
					return;
				}

				sources[i].subscribe(as[i]);
			}
		}

		@Override
		public void request(long n) {
			if (!BackpressureUtils.checkRequest(n, actual)) {
				return;
			}

			int w = winner;
			if (w > 0) {
				subscribers[w - 1].request(n);
			}
			else if (w == 0) {
				for (AmbInnerSubscriber<T> a : subscribers) {
					a.request(n);
				}
			}
		}

		public boolean win(int index) {
			int w = winner;
			if (w == 0) {
				if (WINNER.compareAndSet(this, 0, index)) {
					return true;
				}
				return false;
			}
			return w == index;
		}

		@Override
		public void cancel() {
			if (winner != -1) {
				WINNER.lazySet(this, -1);

				for (AmbInnerSubscriber<T> a : subscribers) {
					if(a != null) {
						a.cancel();
					}
				}
			}
		}
	}

	static final class AmbInnerSubscriber<T> extends BaseSubscriber<T> implements Subscription {

		final AmbBarrier<T>         parent;
		final int                   index;
		final Subscriber<? super T> actual;

		boolean won;

		volatile long missedRequested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<AmbInnerSubscriber> MISSED_REQUESTED =
				AtomicLongFieldUpdater.newUpdater(AmbInnerSubscriber.class, "missedRequested");

		volatile Subscription subscription;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<AmbInnerSubscriber, Subscription> SUBSCRIPTION =
				PlatformDependent.newAtomicReferenceFieldUpdater(AmbInnerSubscriber.class, "subscription");

		static final Subscription CANCELLED = new Subscription() {
			@Override
			public void request(long n) {

			}

			@Override
			public void cancel() {

			}
		};

		public AmbInnerSubscriber(AmbBarrier<T> parent, int index, Subscriber<? super T> actual) {
			this.parent = parent;
			this.index = index;
			this.actual = actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!SUBSCRIPTION.compareAndSet(this, null, s)) {
				s.cancel();
				return;
			}

			long r = MISSED_REQUESTED.getAndSet(this, 0L);
			if (r != 0L) {
				s.request(r);
			}
		}

		@Override
		public void request(long n) {
			Subscription s = SUBSCRIPTION.get(this);
			if (s != null) {
				s.request(n);
			}
			else {
				BackpressureUtils.getAndAdd(MISSED_REQUESTED, this, n);
				s = SUBSCRIPTION.get(this);
				if (s != null && s != CANCELLED) {
					long r = MISSED_REQUESTED.getAndSet(this, 0L);
					if (r != 0L) {
						s.request(r);
					}
				}
			}
		}

		@Override
		public void onNext(T t) {
			if (won) {
				actual.onNext(t);
			}
			else {
				if (parent.win(index)) {
					won = true;
					actual.onNext(t);
				}
				else {
					SUBSCRIPTION.get(this).cancel();
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (won) {
				actual.onError(t);
			}
			else {
				if (parent.win(index)) {
					won = true;
					actual.onError(t);
				}
				else {
					SUBSCRIPTION.get(this).cancel();
				}
			}
		}

		@Override
		public void onComplete() {
			if (won) {
				actual.onComplete();
			}
			else {
				if (parent.win(index)) {
					won = true;
					actual.onComplete();
				}
				else {
					SUBSCRIPTION.get(this).cancel();
				}
			}
		}

		@Override
		public void cancel() {
			Subscription s = SUBSCRIPTION.get(this);
			if (s != CANCELLED) {
				s = SUBSCRIPTION.getAndSet(this, CANCELLED);
				if (s != CANCELLED && s != null) {
					s.cancel();
				}
			}
		}

	}

}