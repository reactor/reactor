/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.processor;

import org.reactivestreams.Processor;
import reactor.core.error.Exceptions;
import reactor.core.support.Resource;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A Shared Processor Service is a {@link Processor} factory eventually sharing one or more internal {@link Processor}.
 * <p>
 * Its purpose is to mutualize some threading and event loop resources, thus creating an (a)sync gateway reproducing
 * the input sequence of signals to their attached subscriber context.
 * Its default behavior will be to request a fair share of the internal
 * {@link Processor} to allow many concurrent use of a single async resource.
 * <p>
 * Alongside building Processor, SharedProcessor can generate unbounded dispatchers as:
 * - a {@link BiConsumer} that schedules the data argument over the  {@link Consumer} task argument.
 * - a {@link Consumer} that schedules  {@link Consumer} task argument.
 * - a {@link Executor} that runs an arbitrary {@link Runnable} task.
 * <p>
 * SharedProcessor maintains a reference count on how many artefacts have been built. Therefore it will automatically
 * shutdown the internal async resource after all references have been released. Each reference will therefore use
 * Processor.onComplete or predefined task signals as {@link Consumer} or {@link Runnable} to effectively clean the
 * underlying
 * processor resources.
 *
 * @param <T> the default typed
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
public final class SharedProcessorService<T> implements Supplier<Processor<T, T>>, Resource {

    /**
     * Determine the underlying reactor processor: synchronous, async (ringbufferProcessor if supported or
     * simpleAsyncProcessor), work async (ringbufferWorkProcessor if supported or simpleWorkProcessor).
     * <p>
     * Note that work services won't support processor creating but will stick to dispatcher creation. This is due
     * to the non concurrent onXXX requirement from processors subscribers where a subscriber here might end up
     * executing in more one thread.
     */
    public enum Type {
        SYNC, ASYNC, WORK
    }


    public static <E> SharedProcessorService<E> create(boolean autoShutdown) {
        return create(RingBufferWorkProcessor.class.getSimpleName(), , autoShutdown);
    }

    /**
     * Singleton delegating consumer for synchronous data dispatchers
     */
    public final static BiConsumer<Object, Consumer<? super Object>> SYNC_DATA_DISPATCHER = new BiConsumer<Object,
      Consumer<? super Object>>() {
        @Override
        public void accept(Object o, Consumer<? super Object> callback) {
            callback.accept(o);
        }
    };

    /**
     * Singleton delegating consumer for synchronous dispatchers
     */
    public final static Consumer<Consumer<? super Void>> SYNC_DISPATCHER = new Consumer<Consumer<? super Void>>() {
        @Override
        public void accept(Consumer<? super Void> callback) {
            callback.accept(null);
        }
    };

    /**
     * Singleton delegating executor for synchronous executor
     */
    public final static Executor SYNC_EXECUTOR = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };


    /**
     * INSTANCE STUFF *
     */

    final private Type                  type;
    final private Processor<Task, Task> processor;

    final private boolean autoShutdown;
    @SuppressWarnings("unused")
    private volatile int refCount = 0;

    private static final AtomicIntegerFieldUpdater<SharedProcessorService> REF_COUNT =
      AtomicIntegerFieldUpdater
        .newUpdater(SharedProcessorService.class, "refCount");

    @Override
    public Processor<T, T> get() {
        return directProcessor();
    }

    public Processor<T, T> directProcessor() {
        return null;
    }

    public <V> Processor<V, V> directProcessor(Class<V> clazz) {
        return null;
    }

    public Consumer<Consumer<? super Void>> dispatcher() {
        switch (type) {
            case SYNC:
                return SYNC_DISPATCHER;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <V> BiConsumer<V, Consumer<? super V>> dataDispatcher(Class<V> clazz) {
        return (BiConsumer<V, Consumer<? super V>>) dataDispatcher();
    }

    @SuppressWarnings("unchecked")
    public BiConsumer<T, Consumer<? super T>> dataDispatcher() {
        if (processor == null || type == Type.SYNC) {
            return (BiConsumer<T, Consumer<? super T>>) SYNC_DATA_DISPATCHER;
        }
        return new BiConsumer<T, Consumer<? super T>>() {
            @Override
            public void accept(T t, Consumer<? super T> consumer) {

            }
        }
    }

    public Executor executor() {
        switch (type) {
            case SYNC:
               return SYNC_EXECUTOR;
        }

        return new Executor() {
            @Override
            public void execute(final Runnable command) {

            }
        };
    }

    @Override
    public boolean awaitAndShutdown() {
        return awaitAndShutdown(-1, TimeUnit.SECONDS);
    }

    @Override
    public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
        if (Resource.class.isAssignableFrom(processor.getClass())) {
            return ((Resource) processor).awaitAndShutdown(timeout, timeUnit);
        }
        throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
    }

    @Override
    public void forceShutdown() {
        if (Resource.class.isAssignableFrom(processor.getClass())) {
            ((Resource) processor).forceShutdown();
            return;
        }
        throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
    }

    @Override
    public boolean alive() {
        if (Resource.class.isAssignableFrom(processor.getClass())) {
            return ((Resource) processor).alive();
        }
        throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
    }

    @Override
    public void shutdown() {
        try {
            processor.onComplete();
            if (Resource.class.isAssignableFrom(processor.getClass())) {
                ((Resource) processor).shutdown();
            }
        } catch (Throwable t) {
            Exceptions.throwIfFatal(t);
            processor.onError(t);
        }
    }

    private SharedProcessorService(Processor<Task, Task> processor, Type type, boolean autoShutdown) {
        this.processor = processor;
        this.type = type;
        this.autoShutdown = autoShutdown;
    }


    /**
     *
     */
    static class TailRecurser {

        private final ArrayList<Task> pile;

        private final int pileSizeIncrement;

        private final Supplier<Task> taskSupplier;

        private final Consumer<Task> pileConsumer;

        private int next = 0;

        public TailRecurser(int backlogSize, Supplier<Task> taskSupplier, Consumer<Task> taskConsumer) {
            this.pileSizeIncrement = backlogSize * 2;
            this.taskSupplier = taskSupplier;
            this.pileConsumer = taskConsumer;
            this.pile = new ArrayList<Task>(pileSizeIncrement);
            ensureEnoughTasks();
        }

        private void ensureEnoughTasks() {
            if (next >= pile.size()) {
                pile.ensureCapacity(pile.size() + pileSizeIncrement);
                for (int i = 0; i < pileSizeIncrement; i++) {
                    pile.add(taskSupplier.get());
                }
            }
        }

        public Task next() {
            ensureEnoughTasks();
            return pile.get(next++);
        }

        public void consumeTasks() {
            if (next > 0) {
                for (int i = 0; i < next; i++) {
                    pileConsumer.accept(pile.get(i));
                }

                for (int i = next - 1; i >= pileSizeIncrement; i--) {
                    pile.remove(i);
                }
                next = 0;
            }
        }
    }

    private static final class Task<T> {

    }
}
