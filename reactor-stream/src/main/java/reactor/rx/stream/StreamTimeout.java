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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.subscriber.SubscriberMultiSubscription;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.rx.subscriber.SerializedSubscriber;

/**
 * Signals a timeout (or switches to another sequence) in case a per-item
 * generated Publisher source fires an item or completes before the next item
 * arrives from the main source.
 *
 * @param <T> the main source type
 * @param <U> the value type for the timeout for the very first item
 * @param <V> the value type for the timeout for the subsequent items
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @since 2.5
 */
public final class StreamTimeout<T, U, V> extends StreamBarrier<T, T> {

    final Supplier<? extends Publisher<U>> firstTimeout;

    final Function<? super T, ? extends Publisher<V>> itemTimeout;

    final Publisher<? extends T> other;

    public StreamTimeout(Publisher<? extends T> source,
            Supplier<? extends Publisher<U>> firstTimeout,
            Function<? super T, ? extends Publisher<V>> itemTimeout) {
        super(source);
        this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
        this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
        this.other = null;
    }

    public StreamTimeout(Publisher<? extends T> source,
            Supplier<? extends Publisher<U>> firstTimeout,
            Function<? super T, ? extends Publisher<V>> itemTimeout,
            Publisher<? extends T> other) {
        super(source);
        this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
        this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
        this.other = Objects.requireNonNull(other, "other");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        SerializedSubscriber<T> serial = new SerializedSubscriber<>(s);

        StreamTimeoutMainSubscriber<T, V> main = new StreamTimeoutMainSubscriber<>(serial, itemTimeout, other);

        serial.onSubscribe(main);

        Publisher<U> firstPublisher;

        try {
            firstPublisher = firstTimeout.get();
        }
        catch (Throwable e) {
            serial.onError(e);
            return;
        }

        if (firstPublisher == null) {
            serial.onError(new NullPointerException("The firstTimeout returned a null Publisher"));
            return;
        }

        StreamTimeoutTimeoutSubscriber ts = new StreamTimeoutTimeoutSubscriber(main, 0L);

        main.setTimeout(ts);

        firstPublisher.subscribe(ts);

        source.subscribe(main);
    }

    static final class StreamTimeoutMainSubscriber<T, V> extends SubscriberMultiSubscription<T, T> {

        final Function<? super T, ? extends Publisher<V>> itemTimeout;

        final Publisher<? extends T> other;

        Subscription s;

        volatile IndexedCancellable timeout;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<StreamTimeoutMainSubscriber, IndexedCancellable> TIMEOUT =
                AtomicReferenceFieldUpdater.newUpdater(StreamTimeoutMainSubscriber.class,
                        IndexedCancellable.class,
                        "timeout");

        volatile long index;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<StreamTimeoutMainSubscriber> INDEX =
                AtomicLongFieldUpdater.newUpdater(StreamTimeoutMainSubscriber.class, "index");

        public StreamTimeoutMainSubscriber(Subscriber<? super T> actual,
                Function<? super T, ? extends Publisher<V>> itemTimeout,
                Publisher<? extends T> other) {
            super(actual);
            this.itemTimeout = itemTimeout;
            this.other = other;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (BackpressureUtils.validate(this.s, s)) {
                this.s = s;

                set(s);
            }
        }

        @Override
        public void onNext(T t) {
            timeout.cancel();

            long idx = index;
            if (idx == Long.MIN_VALUE) {
                s.cancel();
	            Exceptions.onNextDropped(t);
	            return;
            }
            if (!INDEX.compareAndSet(this, idx, idx + 1)) {
                s.cancel();
	            Exceptions.onNextDropped(t);
                return;
            }

            subscriber.onNext(t);

            producedOne();

            Publisher<? extends V> p;

            try {
                p = itemTimeout.apply(t);
            }
            catch (Throwable e) {
                cancel();

                subscriber.onError(e);
                return;
            }

            if (p == null) {
                cancel();

                subscriber.onError(new NullPointerException("The itemTimeout returned a null Publisher"));
                return;
            }

            StreamTimeoutTimeoutSubscriber ts = new StreamTimeoutTimeoutSubscriber(this, idx + 1);

            if (!setTimeout(ts)) {
                return;
            }

            p.subscribe(ts);
        }

        @Override
        public void onError(Throwable t) {
            long idx = index;
            if (idx == Long.MIN_VALUE) {
	            Exceptions.onErrorDropped(t);
                return;
            }
            if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
	            Exceptions.onErrorDropped(t);
                return;
            }

            cancelTimeout();

            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            long idx = index;
            if (idx == Long.MIN_VALUE) {
                return;
            }
            if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
                return;
            }

            cancelTimeout();

            subscriber.onComplete();
        }

        void cancelTimeout() {
            IndexedCancellable s = timeout;
            if (s != CancelledIndexedCancellable.INSTANCE) {
                s = TIMEOUT.getAndSet(this, CancelledIndexedCancellable.INSTANCE);
                if (s != null && s != CancelledIndexedCancellable.INSTANCE) {
                    s.cancel();
                }
            }
        }

        @Override
        public void cancel() {
            index = Long.MIN_VALUE;
            cancelTimeout();
            super.cancel();
        }

        boolean setTimeout(IndexedCancellable newTimeout) {

            for (; ; ) {
                IndexedCancellable currentTimeout = timeout;

                if (currentTimeout == CancelledIndexedCancellable.INSTANCE) {
                    newTimeout.cancel();
                    return false;
                }

                if (currentTimeout != null && currentTimeout.index() >= newTimeout.index()) {
                    newTimeout.cancel();
                    return false;
                }

                if (TIMEOUT.compareAndSet(this, currentTimeout, newTimeout)) {
                    if (currentTimeout != null) {
                        currentTimeout.cancel();
                    }
                    return true;
                }
            }
        }

        void doTimeout(long i) {
            if (index == i && INDEX.compareAndSet(this, i, Long.MIN_VALUE)) {
                handleTimeout();
            }
        }

        void doError(long i, Throwable e) {
            if (index == i && INDEX.compareAndSet(this, i, Long.MIN_VALUE)) {
                super.cancel();

                subscriber.onError(e);
            }
        }

        void handleTimeout() {
            if (other == null) {
                super.cancel();

                subscriber.onError(new TimeoutException());
            }
            else {
                set(EmptySubscription.INSTANCE);

                other.subscribe(new StreamTimeoutOtherSubscriber<>(subscriber, this));
            }
        }
    }

    static final class StreamTimeoutOtherSubscriber<T> implements Subscriber<T> {

        final Subscriber<? super T> actual;

        final SubscriberMultiSubscription<T, T> arbiter;

        public StreamTimeoutOtherSubscriber(Subscriber<? super T> actual, SubscriberMultiSubscription<T, T> arbiter) {
            this.actual = actual;
            this.arbiter = arbiter;
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }

    interface IndexedCancellable {

        long index();

        void cancel();
    }

    enum CancelledIndexedCancellable implements IndexedCancellable {
        INSTANCE;

        @Override
        public long index() {
            return Long.MAX_VALUE;
        }

        @Override
        public void cancel() {

        }

    }

    static final class StreamTimeoutTimeoutSubscriber implements Subscriber<Object>, IndexedCancellable {

        final StreamTimeoutMainSubscriber<?, ?> main;

        final long index;

        volatile Subscription s;

        static final AtomicReferenceFieldUpdater<StreamTimeoutTimeoutSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(StreamTimeoutTimeoutSubscriber.class, Subscription.class, "s");

        public StreamTimeoutTimeoutSubscriber(StreamTimeoutMainSubscriber<?, ?> main, long index) {
            this.main = main;
            this.index = index;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!S.compareAndSet(this, null, s)) {
                s.cancel();
                if (this.s != CancelledSubscription.INSTANCE) {
                    BackpressureUtils.reportSubscriptionSet();
                }
            }
        }

        @Override
        public void onNext(Object t) {
            s.cancel();

            main.doTimeout(index);
        }

        @Override
        public void onError(Throwable t) {
            main.doError(index, t);
        }

        @Override
        public void onComplete() {
            main.doTimeout(index);
        }

        @Override
        public void cancel() {
            Subscription a = s;
            if (a != CancelledSubscription.INSTANCE) {
                a = S.getAndSet(this, CancelledSubscription.INSTANCE);
                if (a != null && a != CancelledSubscription.INSTANCE) {
                    a.cancel();
                }
            }
        }

        @Override
        public long index() {
            return index;
        }
    }
}
