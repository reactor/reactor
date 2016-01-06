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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.EmitterProcessor;
import reactor.core.subscriber.SubscriberDeferSubscription;
import reactor.core.subscriber.SubscriberMultiSubscription;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.subscriber.SerializedSubscriber;

/**
 * retries a source when a companion sequence signals
 * an item in response to the main's error signal
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 *
 * @param <T> the source value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @since 2.5
 */
public final class StreamRetryWhen<T> extends StreamBarrier<T, T> {

    final Function<Stream<Throwable>, ? extends Publisher<? extends Object>> whenSourceFactory;

    public StreamRetryWhen(Publisher<? extends T> source,
            Function<Stream<Throwable>, ? extends Publisher<? extends Object>> whenSourceFactory) {
        super(source);
        this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        StreamRetryWhenOtherSubscriber other = new StreamRetryWhenOtherSubscriber();

        SerializedSubscriber<T> serial = new SerializedSubscriber<>(s);

        StreamRetryWhenMainSubscriber<T> main =
                new StreamRetryWhenMainSubscriber<>(serial, other.completionSignal, source);
        other.main = main;

        serial.onSubscribe(main);

        Publisher<? extends Object> p;

        try {
            p = whenSourceFactory.apply(other);
        }
        catch (Throwable e) {
            s.onError(e);
            return;
        }

        if (p == null) {
            s.onError(new NullPointerException("The whenSourceFactory returned a null Publisher"));
            return;
        }

        p.subscribe(other);

        if (!main.cancelled) {
            source.subscribe(main);
        }
    }

    static final class StreamRetryWhenMainSubscriber<T> extends SubscriberMultiSubscription<T, T> {

        final SubscriberDeferSubscription<T, T> otherArbiter;

        final Subscriber<Throwable> signaller;

        final Publisher<? extends T> source;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<StreamRetryWhenMainSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(StreamRetryWhenMainSubscriber.class, "wip");

        volatile boolean cancelled;

        public StreamRetryWhenMainSubscriber(Subscriber<? super T> actual,
                Subscriber<Throwable> signaller,
                Publisher<? extends T> source) {
            super(actual);
            this.signaller = signaller;
            this.source = source;
            this.otherArbiter = new SubscriberDeferSubscription<>(null);
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;

            cancelWhen();

            super.cancel();
        }

        void cancelWhen() {
            otherArbiter.cancel();
        }

        public void setWhen(Subscription w) {
            otherArbiter.set(w);
        }

        @Override
        public void onNext(T t) {
            subscriber.onNext(t);

            producedOne();
        }

        @Override
        public void onError(Throwable t) {
            otherArbiter.request(1);

            signaller.onNext(t);
        }

        @Override
        public void onComplete() {
            otherArbiter.cancel();

            subscriber.onComplete();
        }

        void resubscribe() {
            if (WIP.getAndIncrement(this) == 0) {
                do {
                    if (cancelled) {
                        return;
                    }

                    source.subscribe(this);

                }
                while (WIP.decrementAndGet(this) != 0);
            }
        }

        void whenError(Throwable e) {
            cancelled = true;
            super.cancel();

            subscriber.onError(e);
        }

        void whenComplete() {
            cancelled = true;
            super.cancel();

            subscriber.onComplete();
        }
    }

    static final class StreamRetryWhenOtherSubscriber extends Stream<Throwable> implements Subscriber<Object> {

        StreamRetryWhenMainSubscriber<?> main;

        final EmitterProcessor<Throwable> completionSignal = new EmitterProcessor<>();

        @Override
        public void onSubscribe(Subscription s) {
            main.setWhen(s);
        }

        @Override
        public void onNext(Object t) {
            main.resubscribe();
        }

        @Override
        public void onError(Throwable t) {
            main.whenError(t);
        }

        @Override
        public void onComplete() {
            main.whenComplete();
        }

        @Override
        public void subscribe(Subscriber<? super Throwable> s) {
            completionSignal.subscribe(s);
        }
    }
}
