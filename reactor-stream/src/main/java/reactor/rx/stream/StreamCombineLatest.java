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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.Function;
import reactor.fn.Supplier;

/**
 * Combines the latest values from multiple sources through a function.
 *
 * @param <T> the value type of the sources
 * @param <R> the result type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamCombineLatest<T, R> extends reactor.rx.Stream<R>
        implements ReactiveState.Factory, ReactiveState.LinkedUpstreams {

    final Publisher<? extends T>[] array;

    final Iterable<? extends Publisher<? extends T>> iterable;

    final Function<Object[], R> combiner;

    final Supplier<? extends Queue<SourceAndArray>> queueSupplier;

    final int bufferSize;

    public StreamCombineLatest(Publisher<? extends T>[] array,
            Function<Object[], R> combiner,
            Supplier<? extends Queue<SourceAndArray>> queueSupplier,
            int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize > 0 required but it was " + bufferSize);
        }

        this.array = Objects.requireNonNull(array, "array");
        this.iterable = null;
        this.combiner = Objects.requireNonNull(combiner, "combiner");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.bufferSize = bufferSize;
    }

    public StreamCombineLatest(Iterable<? extends Publisher<? extends T>> iterable,
            Function<Object[], R> combiner,
            Supplier<? extends Queue<SourceAndArray>> queueSupplier,
            int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize > 0 required but it was " + bufferSize);
        }

        this.array = null;
        this.iterable = Objects.requireNonNull(iterable, "iterable");
        this.combiner = Objects.requireNonNull(combiner, "combiner");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.bufferSize = bufferSize;
    }

    @Override
    public Iterator<?> upstreams() {
        return iterable != null ? iterable.iterator() : Arrays.asList(array)
                                                              .iterator();
    }

    @Override
    public long upstreamsCount() {
        return array != null ? array.length : -1L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super R> s) {
        Publisher<? extends T>[] a = array;
        int n;
        if (a == null) {
            n = 0;
            a = new Publisher[8];

            Iterator<? extends Publisher<? extends T>> it;

            try {
                it = iterable.iterator();
            }
            catch (Throwable e) {
                EmptySubscription.error(s, e);
                return;
            }

            if (it == null) {
                EmptySubscription.error(s, new NullPointerException("The iterator returned is null"));
                return;
            }

            for (; ; ) {

                boolean b;

                try {
                    b = it.hasNext();
                }
                catch (Throwable e) {
                    EmptySubscription.error(s, e);
                    return;
                }

                if (!b) {
                    break;
                }

                Publisher<? extends T> p;

                try {
                    p = it.next();
                }
                catch (Throwable e) {
                    EmptySubscription.error(s, e);
                    return;
                }

                if (p == null) {
                    EmptySubscription.error(s,
                            new NullPointerException("The Publisher returned by the iterator is " + "null"));
                    return;
                }

                if (n == a.length) {
                    Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
                    System.arraycopy(a, 0, c, 0, n);
                    a = c;
                }
                a[n++] = p;
            }

        }
        else {
            n = a.length;
        }

        if (n == 0) {
            EmptySubscription.complete(s);
            return;
        }
        if (n == 1) {
            new StreamMap<>(a[0], new Function<T, R>() {
                @Override
                public R apply(T t) {
                    return combiner.apply(new Object[]{t});
                }
            }).subscribe(s);
            return;
        }

        Queue<SourceAndArray> queue;

        try {
            queue = queueSupplier.get();
        }
        catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        if (queue == null) {
            EmptySubscription.error(s, new NullPointerException("The queueSupplier returned a null queue"));
            return;
        }

        StreamCombineLatestCoordinator<T, R> coordinator =
                new StreamCombineLatestCoordinator<>(s, combiner, n, queue, bufferSize);

        s.onSubscribe(coordinator);

        coordinator.subscribe(a, n);
    }

    static final class StreamCombineLatestCoordinator<T, R> implements Subscription, LinkedUpstreams, ActiveDownstream {

        final Subscriber<? super R> actual;

        final Function<Object[], R> combiner;

        final StreamCombineLatestInner<T>[] subscribers;

        final Queue<SourceAndArray> queue;

        final Object[] latest;

        int nonEmptySources;

        int completedSources;

        volatile boolean cancelled;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<StreamCombineLatestCoordinator> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(StreamCombineLatestCoordinator.class, "requested");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<StreamCombineLatestCoordinator> WIP =
                AtomicIntegerFieldUpdater.newUpdater(StreamCombineLatestCoordinator.class, "wip");

        volatile boolean done;

        volatile Throwable error;

        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<StreamCombineLatestCoordinator, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(StreamCombineLatestCoordinator.class, Throwable.class, "error");

        static final Throwable TERMINAL_ERROR = new Throwable();

        public StreamCombineLatestCoordinator(Subscriber<? super R> actual,
                Function<Object[], R> combiner,
                int n,
                Queue<SourceAndArray> queue,
                int bufferSize) {
            this.actual = actual;
            this.combiner = combiner;
            @SuppressWarnings("unchecked") StreamCombineLatestInner<T>[] a = new StreamCombineLatestInner[n];
            for (int i = 0; i < n; i++) {
                a[i] = new StreamCombineLatestInner<>(this, i, bufferSize);
            }
            this.subscribers = a;
            this.latest = new Object[n];
            this.queue = queue;
        }

        @Override
        public void request(long n) {
            if (BackpressureUtils.validate(n)) {
                BackpressureUtils.addAndGet(REQUESTED, this, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public Iterator<?> upstreams() {
            return Arrays.asList(subscribers)
                         .iterator();
        }

        @Override
        public long upstreamsCount() {
            return subscribers.length;
        }

        void subscribe(Publisher<? extends T>[] sources, int n) {
            StreamCombineLatestInner<T>[] a = subscribers;

            for (int i = 0; i < n; i++) {
                if (done || cancelled) {
                    return;
                }
                sources[i].subscribe(a[i]);
            }
        }

        void innerValue(int index, T value) {
            synchronized (this) {
                Object[] os = latest;

                int localNonEmptySources = nonEmptySources;

                if (os[index] == null) {
                    localNonEmptySources++;
                    nonEmptySources = localNonEmptySources;
                }

                os[index] = value;

                if (os.length == localNonEmptySources) {
                    SourceAndArray sa = new SourceAndArray(subscribers[index], os.clone());

                    queue.offer(sa);
                }
                else {
                    return;
                }
            }

            drain();
        }

        void innerComplete(int index) {
            synchronized (this) {
                Object[] os = latest;

                if (os[index] != null) {
                    int localCompletedSources = completedSources + 1;

                    if (localCompletedSources == os.length) {
                        done = true;
                    }
                    else {
                        completedSources = localCompletedSources;
                        return;
                    }
                }
                else {
                    done = true;
                }
            }
            drain();
        }

        void innerError(Throwable e) {

            for (; ; ) {
                Throwable ex = error;

                if (ex == TERMINAL_ERROR) {
                    Exceptions.onErrorDropped(ex);
                    return;
                }

                Throwable u;
                if (ex == null) {
                    u = e;
                }
                else {
                    u = new RuntimeException("Multiple exceptions");
                    u.addSuppressed(ex);
                    u.addSuppressed(e);
                }

                if (ERROR.compareAndSet(this, ex, u)) {
                    done = true;
                    break;
                }
            }

            drain();
        }

        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }

            final Subscriber<? super R> a = actual;
            final Queue<SourceAndArray> q = queue;

            int missed = 1;

            for (; ; ) {

                long r = requested;
                long e = 0L;

                while (e != r) {
                    boolean d = done;

                    SourceAndArray v = q.poll();

                    boolean empty = v == null;

                    if (checkTerminated(d, empty, a, q)) {
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    R w;

                    try {
                        w = combiner.apply(v.array);
                    }
                    catch (Throwable ex) {
                        innerError(ex);
                        continue;
                    }

                    if (w == null) {
                        innerError(new NullPointerException("The combiner returned a null value"));
                        continue;
                    }

                    a.onNext(w);

                    v.source.requestOne();

                    e++;
                }

                if (e == r) {
                    if (checkTerminated(done, q.isEmpty(), a, q)) {
                        return;
                    }
                }

                if (e != 0L && r != Long.MAX_VALUE) {
                    REQUESTED.addAndGet(this, -e);
                }

                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
            if (cancelled) {
                cancelAll();
                q.clear();
                return true;
            }

            if (d) {
                Throwable e = ERROR.getAndSet(this, TERMINAL_ERROR);

                if (e != null) {
                    cancelAll();
                    q.clear();
                    a.onError(e);
                    return true;
                }
                else if (empty) {
                    cancelAll();

                    a.onComplete();
                    return true;
                }
            }
            return false;
        }

        void cancelAll() {
            for (StreamCombineLatestInner<T> inner : subscribers) {
                inner.cancel();
            }
        }
    }

    static final class StreamCombineLatestInner<T>
            implements Subscriber<T>, Inner, UpstreamDemand, DownstreamDemand, UpstreamPrefetch, Upstream, Downstream {

        final StreamCombineLatestCoordinator<T, ?> parent;

        final int index;

        final int limit;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<StreamCombineLatestInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(StreamCombineLatestInner.class, Subscription.class, "s");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<StreamCombineLatestInner> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(StreamCombineLatestInner.class, "requested");

        int produced;

        public StreamCombineLatestInner(StreamCombineLatestCoordinator<T, ?> parent, int index, int bufferSize) {
            this.parent = parent;
            this.index = index;
            this.requested = bufferSize;
            this.limit = bufferSize - (bufferSize >> 2);
        }

        @Override
        public void onSubscribe(Subscription s) {
            Objects.requireNonNull(s, "s");
            Subscription a = this.s;
            if (a == CancelledSubscription.INSTANCE) {
                s.cancel();
                return;
            }
            if (a != null) {
                s.cancel();
                BackpressureUtils.reportSubscriptionSet();
                return;
            }

            if (S.compareAndSet(this, null, s)) {

                long r = REQUESTED.getAndSet(this, 0L);

                if (r != 0L) {
                    s.request(r);
                }

                return;
            }

            a = this.s;

            if (a != CancelledSubscription.INSTANCE) {
                s.cancel();
            }
            else {
                BackpressureUtils.reportSubscriptionSet();
            }
        }

        @Override
        public void onNext(T t) {
            parent.innerValue(index, t);
        }

        @Override
        public void onError(Throwable t) {
            parent.innerError(t);
        }

        @Override
        public void onComplete() {
            parent.innerComplete(index);
        }

        public void cancel() {
            Subscription a = s;
            if (a != CancelledSubscription.INSTANCE) {
                a = S.getAndSet(this, CancelledSubscription.INSTANCE);
                if (a != null && a != CancelledSubscription.INSTANCE) {
                    a.cancel();
                }
            }
        }

        public void requestOne() {

            int p = produced + 1;
            if (p == limit) {
                produced = 0;
                Subscription a = s;
                if (a != null) {
                    a.request(p);
                }
                else {
                    BackpressureUtils.addAndGet(REQUESTED, this, p);

                    a = s;

                    if (a != null) {
                        long r = REQUESTED.getAndSet(this, 0L);

                        if (r != 0L) {
                            a.request(r);
                        }
                    }
                }
            }
            else {
                produced = p;
            }

        }

        @Override
        public Object downstream() {
            return parent;
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public long limit() {
            return limit;
        }

        @Override
        public long expectedFromUpstream() {
            return limit - produced;
        }
    }

    /**
     * The queue element type for internal use with StreamCombineLatest.
     */
    public static final class SourceAndArray {

        final StreamCombineLatestInner<?> source;
        final Object[]                    array;

        SourceAndArray(StreamCombineLatestInner<?> source, Object[] array) {
            this.source = source;
            this.array = array;
        }
    }
}
