package reactor.core.dispatch;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.alloc.Recyclable;
import reactor.core.dispatch.wait.WaitingMood;
import reactor.core.processor.ImmutableSignal;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.RingBuffer;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a {@link reactor.core.Dispatcher} that uses a {@link RingBuffer} to queue tasks to execute.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Anatoly Kadyshev
 */
public class RingBufferDispatcher3 implements Dispatcher, WaitingMood {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final RingBufferProcessor<Task> processor;

    private final TailRecurser tailRecurser;

    private final WaitingMood waitingMood;

    protected static final int DEFAULT_BUFFER_SIZE = 1024;

    /**
     * Creates a new {@code RingBufferDispatcher} with the given {@code name}. It will use a RingBuffer with 1024 slots,
     * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
     * wait
     * strategy}.
     *
     * @param name The name of the dispatcher.
     */
    public RingBufferDispatcher3(String name) {
        this(name, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new {@code RingBufferDispatcher} with the given {@code name} and {@param bufferSize},
     * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
     * wait
     * strategy}.
     *
     * @param name       The name of the dispatcher
     * @param bufferSize The size to configure the ring buffer with
     */
    public RingBufferDispatcher3(String name, int bufferSize) {
        this(name, bufferSize, null);
    }

    /**
     * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
     * {@code bufferSize} slots, configured with a producer type of {@link ProducerType#MULTI MULTI}
     * and a {@link BlockingWaitStrategy blocking wait. A given @param uncaughtExceptionHandler} will catch anything not
     * handled e.g. by the owning {@code reactor.bus.EventBus} or {@code reactor.rx.Stream}.
     *
     * @param name                     The name of the dispatcher
     * @param bufferSize               The size to configure the ring buffer with
     * @param uncaughtExceptionHandler The last resort exception handler
     */
    public RingBufferDispatcher3(String name,
                                int bufferSize,
                                final Consumer<Throwable> uncaughtExceptionHandler) {
        this(name, bufferSize, uncaughtExceptionHandler, ProducerType.MULTI, new BlockingWaitStrategy());
    }

    /**
     * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
     * {@code bufferSize} slots, configured with the given {@code producerType}, {@param uncaughtExceptionHandler}
     * and {@code waitStrategy}. A null {@param uncaughtExceptionHandler} will make this dispatcher logging such
     * exceptions.
     *
     * @param name                     The name of the dispatcher
     * @param bufferSize               The size to configure the ring buffer with
     * @param producerType             The producer type to configure the ring buffer with
     * @param waitStrategy             The wait strategy to configure the ring buffer with
     * @param uncaughtExceptionHandler The last resort exception handler
     */
    public RingBufferDispatcher3(String name,
                                 int bufferSize,
                                 final Consumer<Throwable> uncaughtExceptionHandler,
                                 ProducerType producerType,
                                 WaitStrategy waitStrategy) {

        this.waitingMood = WaitingMood.class.isAssignableFrom(waitStrategy.getClass()) ? (WaitingMood) waitStrategy : null;

        Supplier<Task> taskSupplier = new Supplier<Task>() {
            @Override
            public Task get() {
                return new Task();
            }
        };

        this.tailRecurser = new TailRecurser(bufferSize, taskSupplier, new Consumer<Task>() {
            @Override
            public void accept(Task task) {
                route(task);
            }
        });

        if (ProducerType.MULTI.equals(producerType)) {
            this.processor = RingBufferProcessor.share(name, bufferSize, waitStrategy, taskSupplier);
        } else {
            this.processor = RingBufferProcessor.create(name, bufferSize, waitStrategy, taskSupplier);
        }

        this.processor.subscribe(createSubscriber(uncaughtExceptionHandler));
    }

    @NotNull
    private Subscriber<Task> createSubscriber(final Consumer<Throwable> uncaughtExceptionHandler) {
        return new Subscriber<Task>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Task task) {
                route(task);
                tailRecurser.consumeTasks();
            }

            @Override
            public void onError(Throwable t) {
                if (uncaughtExceptionHandler != null) {
                    uncaughtExceptionHandler.accept(t);
                } else {
                    log.error(t.getMessage(), t);
                    //TODO: Should we also invoke Environment.get().routeError(t)?
                }
            }

            @Override
            public void onComplete() {
            }

        };
    }

    @SuppressWarnings("unchecked")
    private void route(Task task) {
        try {
            task.eventConsumer.accept(task.data);
        } catch (Throwable t) {
            if (task.errorConsumer != null) {
                task.errorConsumer.accept(t);
            } else if (Environment.alive()) {
                //TODO: Should we route the exception into uncaughtExceptionHandler?
                Environment.get().routeError(t);
            }
        } finally {
            task.recycle();
        }
    }

    @Override
    public <E> void dispatch(E data, Consumer<E> eventConsumer, Consumer<Throwable> errorConsumer) {
        Assert.isTrue(alive(), "This Dispatcher has been shut down.");
        Assert.isTrue(eventConsumer != null, "The signal consumer has not been passed.");

        if (!inContext()) {
            ImmutableSignal<Task> signal = processor.next();
            Task task = signal.getValue();
            task.setData(data)
                    .setEventConsumer(eventConsumer)
                    .setErrorConsumer(errorConsumer);
            processor.publish(signal);
        } else {
            Task task = tailRecurser.next();
            task.setData(data)
                    .setEventConsumer(eventConsumer)
                    .setErrorConsumer(errorConsumer);
        }
    }

    @Override
    public <E> void tryDispatch(E data, Consumer<E> eventConsumer, Consumer<Throwable> errorConsumer) throws InsufficientCapacityException {
        Assert.isTrue(alive(), "This Dispatcher has been shut down.");
        Assert.isTrue(eventConsumer != null, "The signal consumer has not been passed.");

        ImmutableSignal<Task> signal = processor.tryNext();

        Task task = signal.getValue();
        task.setData(data)
                .setEventConsumer(eventConsumer)
                .setErrorConsumer(errorConsumer);

        processor.publish(signal);
    }

    @Override
    public long remainingSlots() {
        return processor.remainingCapacity();
    }

    @Override
    public long backlogSize() {
        return processor.getCapacity();
    }

    @Override
    public boolean supportsOrdering() {
        return true;
    }

    @Override
    public boolean inContext() {
        return processor.isInContext();
    }

    @Override
    public void execute(final Runnable command) {
        dispatch(null, new Consumer<Task>() {
            @Override
            public void accept(Task task) {
                command.run();
            }
        }, null);
    }

    @Override
    public boolean alive() {
        return processor.alive();
    }

    @Override
    public void shutdown() {
        processor.shutdown();
    }

    @Override
    public boolean awaitAndShutdown() {
        return processor.awaitAndShutdown();
    }

    @Override
    public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
        return processor.awaitAndShutdown(timeout, timeUnit);
    }

    @Override
    public void forceShutdown() {
        processor.forceShutdown();
    }

    @Override
    public void nervous() {
        if (waitingMood != null) {
            execute(new Runnable() {
                @Override
                public void run() {
                    waitingMood.nervous();
                }
            });
        }
    }

    @Override
    public void calm() {
        if (waitingMood != null) {
            execute(new Runnable() {
                @Override
                public void run() {
                    waitingMood.calm();
                }
            });
        }
    }

    static class TailRecurser {

        private final ArrayList<Task> pile;

        private final int pileSizeIncrement;

        private final Supplier<Task> taskSupplier;

        private final Consumer<Task> pileConsumer;

        private int next = 0;

        TailRecurser(int backlogSize, Supplier<Task> taskSupplier,
                     Consumer<Task> taskConsumer) {
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

    static class Task implements Recyclable {

        private Consumer eventConsumer;

        private Consumer<Throwable> errorConsumer;

        private Object data;

        public Task setData(Object data) {
            this.data = data;
            return this;
        }

        public Task setEventConsumer(Consumer eventConsumer) {
            this.eventConsumer = eventConsumer;
            return this;
        }

        public Task setErrorConsumer(Consumer<Throwable> errorConsumer) {
            this.errorConsumer = errorConsumer;
            return this;
        }

        @Override
        public void recycle() {
            eventConsumer = null;
            errorConsumer = null;
            data = null;
        }

    }
}
