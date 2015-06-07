package reactor.core.dispatch;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.processor.ImmutableSignal;
import reactor.core.processor.RingBufferProcessor;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class RingBufferDispatcher2 implements Dispatcher {

    private final RingBufferProcessor<Task> processor;

    public static class Task<E> {

        Consumer<E> eventConsumer;

        Consumer<Throwable> errorConsumer;

        E data;

    }

    public RingBufferDispatcher2(String name,
                                int bufferSize,
                                final Consumer<Throwable> uncaughtExceptionHandler,
                                ProducerType producerType,
                                WaitStrategy waitStrategy) {

        //TODO: Remove
        if (!ProducerType.MULTI.equals(producerType)) {
            throw new IllegalArgumentException();
        }

        this.processor = RingBufferProcessor.share(name, bufferSize, waitStrategy, new Supplier<Task>() {
            @Override
            public Task get() {
                return new Task();
            }
        });

        this.processor.subscribe(new Subscriber<Task>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Task event) {
                try {
                    event.eventConsumer.accept(event.data);
                } catch (Throwable t) {
                    event.errorConsumer.accept(t);
                }
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }

        });
    }

    @Override
    public <E> void dispatch(E data, Consumer<E> eventConsumer, Consumer<Throwable> errorConsumer) {
        ImmutableSignal<Task> signal = processor.next();

        Task<E> task = signal.getValue();
        task.data = data;
        task.eventConsumer = eventConsumer;
        task.errorConsumer = errorConsumer;

        processor.publish(signal);
    }

    @Override
    public <E> void tryDispatch(E data, Consumer<E> eventConsumer, Consumer<Throwable> errorConsumer) throws InsufficientCapacityException {
        ImmutableSignal<Task> signal = processor.tryNext();

        Task<E> task = signal.getValue();
        task.data = data;
        task.eventConsumer = eventConsumer;
        task.errorConsumer = errorConsumer;

        processor.publish(signal);
    }

    @Override
    public long remainingSlots() {
        return processor.remainingCapacity();
    }

    @Override
    public long backlogSize() {
        return 0;
    }

    @Override
    public boolean supportsOrdering() {
        return true;
    }

    @Override
    public boolean inContext() {
        return false;
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
}
