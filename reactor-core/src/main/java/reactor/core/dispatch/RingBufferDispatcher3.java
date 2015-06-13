package reactor.core.dispatch;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.alloc.Recyclable;
import reactor.core.processor.ImmutableSignal;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class RingBufferDispatcher3 implements Dispatcher {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final RingBufferProcessor<Task> processor;

    private final TailRecurser tailRecurser;

    public RingBufferDispatcher3(String name,
                                 int bufferSize,
                                 final Consumer<Throwable> uncaughtExceptionHandler,
                                 ProducerType producerType,
                                 WaitStrategy waitStrategy) {

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

        this.processor.subscribe(new Subscriber<Task>() {

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
                }
            }

            @Override
            public void onComplete() {
            }

        });
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

    static class TailRecurser {

        private final ArrayList<Task> pile;

        private final int pileGrowthDelta;

        private final Supplier<Task> taskSupplier;

        private final Consumer<Task> pileConsumer;

        private int next = 0;

        TailRecurser(int backlogSize, Supplier<Task> taskSupplier,
                     Consumer<Task> taskConsumer) {
            this.pileGrowthDelta = backlogSize * 2;
            this.taskSupplier = taskSupplier;
            this.pileConsumer = taskConsumer;
            this.pile = new ArrayList<Task>(pileGrowthDelta);
            ensureEnoughTasks();
        }

        private void ensureEnoughTasks() {
            if (next >= pile.size()) {
                pile.ensureCapacity(pile.size() + pileGrowthDelta);
                for (int i = 0; i < pileGrowthDelta; i++) {
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

                for (int i = next - 1; i >= pileGrowthDelta; i--) {
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
