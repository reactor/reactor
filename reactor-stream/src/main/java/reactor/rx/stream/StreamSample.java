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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.support.BackpressureUtils;
import reactor.rx.subscriber.SerializedSubscriber;

/**
 * Samples the main source and emits its latest value whenever the other Publisher
 * signals a value.
 * <p>
 * <p>
 * Termination of either Publishers will result in termination for the Subscriber
 * as well.
 * <p>
 * <p>
 * Both Publishers will run in unbounded mode because the backpressure
 * would interfere with the sampling precision.
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @since 2.5
 */
public final class StreamSample<T, U> extends StreamBarrier<T, T> {

    final Publisher<U> other;

    public StreamSample(Publisher<? extends T> source, Publisher<U> other) {
        super(source);
        this.other = Objects.requireNonNull(other, "other");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        Subscriber<T> serial = new SerializedSubscriber<>(s);

        StreamSampleMainSubscriber<T> main = new StreamSampleMainSubscriber<>(serial);

        s.onSubscribe(main);

        other.subscribe(new StreamSampleOtherSubscriber<>(main));

        source.subscribe(main);
    }

    static final class StreamSampleMainSubscriber<T> implements Subscriber<T>, Subscription {

        final Subscriber<? super T> actual;

        volatile T value;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<StreamSampleMainSubscriber, Object> VALUE =
                AtomicReferenceFieldUpdater.newUpdater(StreamSampleMainSubscriber.class, Object.class, "value");

        volatile Subscription main;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<StreamSampleMainSubscriber, Subscription> MAIN =
                AtomicReferenceFieldUpdater.newUpdater(StreamSampleMainSubscriber.class, Subscription.class, "main");

        volatile Subscription other;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<StreamSampleMainSubscriber, Subscription> OTHER =
                AtomicReferenceFieldUpdater.newUpdater(StreamSampleMainSubscriber.class, Subscription.class, "other");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<StreamSampleMainSubscriber> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(StreamSampleMainSubscriber.class, "requested");

        public StreamSampleMainSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!MAIN.compareAndSet(this, null, s)) {
                s.cancel();
                if (main != CancelledSubscription.INSTANCE) {
                    BackpressureUtils.reportSubscriptionSet();
                }
                return;
            }
            s.request(Long.MAX_VALUE);
        }

        void cancelMain() {
            Subscription s = main;
            if (s != CancelledSubscription.INSTANCE) {
                s = MAIN.getAndSet(this, CancelledSubscription.INSTANCE);
                if (s != null && s != CancelledSubscription.INSTANCE) {
                    s.cancel();
                }
            }
        }

        void cancelOther() {
            Subscription s = other;
            if (s != CancelledSubscription.INSTANCE) {
                s = OTHER.getAndSet(this, CancelledSubscription.INSTANCE);
                if (s != null && s != CancelledSubscription.INSTANCE) {
                    s.cancel();
                }
            }
        }

        void setOther(Subscription s) {
            if (!OTHER.compareAndSet(this, null, s)) {
                s.cancel();
                if (other != CancelledSubscription.INSTANCE) {
                    BackpressureUtils.reportSubscriptionSet();
                }
                return;
            }
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void request(long n) {
            if (BackpressureUtils.validate(n)) {
                BackpressureUtils.addAndGet(REQUESTED, this, n);
            }
        }

        @Override
        public void cancel() {
            cancelMain();
            cancelOther();
        }

        @Override
        public void onNext(T t) {
            value = t;
        }

        @Override
        public void onError(Throwable t) {
            cancelOther();

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            cancelOther();

            actual.onComplete();
        }

        @SuppressWarnings("unchecked")
        T getAndNullValue() {
            return (T) VALUE.getAndSet(this, null);
        }

        void decrement() {
            REQUESTED.decrementAndGet(this);
        }
    }

    static final class StreamSampleOtherSubscriber<T, U> implements Subscriber<U> {

        final StreamSampleMainSubscriber<T> main;

        public StreamSampleOtherSubscriber(StreamSampleMainSubscriber<T> main) {
            this.main = main;
        }

        @Override
        public void onSubscribe(Subscription s) {
            main.setOther(s);
        }

        @Override
        public void onNext(U t) {
            StreamSampleMainSubscriber<T> m = main;

            T v = m.getAndNullValue();

            if (v != null) {
                if (m.requested != 0L) {
                    m.actual.onNext(v);

                    if (m.requested != Long.MAX_VALUE) {
                        m.decrement();
                    }
                    return;
                }

                m.cancel();

                m.actual.onError(new IllegalStateException("Can't signal value due to lack of requests"));
            }
        }

        @Override
        public void onError(Throwable t) {
            StreamSampleMainSubscriber<T> m = main;

            m.cancelMain();

            m.actual.onError(t);
        }

        @Override
        public void onComplete() {
            StreamSampleMainSubscriber<T> m = main;

            m.cancelMain();

            m.actual.onComplete();
        }


    }
}
