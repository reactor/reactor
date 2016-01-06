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

import java.util.Iterator;
import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.fn.BiFunction;

/**
 * Pairwise combines elements of a publisher and an iterable sequence through a function.
 *
 * @param <T> the main source value type
 * @param <U> the iterable source value type
 * @param <R> the result type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @since 2.5
 */
public final class StreamZipIterable<T, U, R> extends StreamBarrier<T, R> {

	final Iterable<? extends U> other;

	final BiFunction<? super T, ? super U, ? extends R> zipper;

	public StreamZipIterable(Publisher<? extends T> source,
			Iterable<? extends U> other,
			BiFunction<? super T, ? super U, ? extends R> zipper) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.zipper = Objects.requireNonNull(zipper, "zipper");
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		Iterator<? extends U> it;

		try {
			it = other.iterator();
		}
		catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}

		if (it == null) {
			EmptySubscription.error(s, new NullPointerException("The other iterable produced a null iterator"));
			return;
		}

		boolean b;

		try {
			b = it.hasNext();
		}
		catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}

		if (!b) {
			EmptySubscription.complete(s);
			return;
		}

		source.subscribe(new StreamZipSubscriber<>(s, it, zipper));
	}

	static final class StreamZipSubscriber<T, U, R> implements Subscriber<T> {

		final Subscriber<? super R> actual;

		final Iterator<? extends U> it;

		final BiFunction<? super T, ? super U, ? extends R> zipper;

		Subscription s;

		boolean done;

		public StreamZipSubscriber(Subscriber<? super R> actual,
				Iterator<? extends U> it,
				BiFunction<? super T, ? super U, ? extends R> zipper) {
			this.actual = actual;
			this.it = it;
			this.zipper = zipper;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(s);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			U u;

			try {
				u = it.next();
			}
			catch (Throwable e) {
				done = true;
				s.cancel();

				actual.onError(e);
				return;
			}

			R r;

			try {
				r = zipper.apply(t, u);
			}
			catch (Throwable e) {
				done = true;
				s.cancel();

				actual.onError(e);
				return;
			}

			if (r == null) {
				done = true;
				s.cancel();

				actual.onError(new NullPointerException("The zipper returned a null value"));
				return;
			}

			actual.onNext(r);

			boolean b;

			try {
				b = it.hasNext();
			}
			catch (Throwable e) {
				done = true;
				s.cancel();

				actual.onError(e);
				return;
			}

			if (!b) {
				done = true;
				s.cancel();
				actual.onComplete();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			actual.onComplete();
		}
	}
}
