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

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Predicate;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @since 2.5
 */
public final class StreamFilter<T> extends StreamBarrier<T, T> {

    final Predicate<? super T> predicate;

    public StreamFilter(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    public Predicate<? super T> predicate() {
        return predicate;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new StreamFilterSubscriber<>(s, predicate));
    }

    static final class StreamFilterSubscriber<T>
            implements Subscriber<T>, Downstream, FeedbackLoop, ActiveUpstream, Upstream {

        final Subscriber<? super T> actual;

        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        public StreamFilterSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
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

            boolean b;

            try {
                b = predicate.test(t);
            }
            catch (Throwable e) {
                s.cancel();

                onError(e);
                return;
            }
            if (b) {
                actual.onNext(t);
            }
            else {
                s.request(1);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                Exceptions.onErrorDropped(t);
                return;
            }
            done = true;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }

        @Override
        public boolean isStarted() {
            return s != null && !done;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object delegateInput() {
            return predicate;
        }

        @Override
        public Object delegateOutput() {
            return null;
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}
