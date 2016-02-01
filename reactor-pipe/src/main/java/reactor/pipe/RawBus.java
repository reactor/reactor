package reactor.pipe;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.reactivestreams.Processor;
import reactor.bus.AbstractBus;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registry;
import reactor.bus.routing.Router;
import reactor.core.subscriber.Subscribers;
import reactor.core.subscriber.SubscriptionWithContext;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.pipe.registry.DelayedRegistration;
import reactor.pipe.stream.FirehoseSubscription;

public class RawBus<K, V> extends AbstractBus<K, V> {

    private final Processor<Runnable, Runnable> processor;
    private final ThreadLocal<Boolean>          inDispatcherContext;
    private final FirehoseSubscription          firehoseSubscription;

    public RawBus(@Nonnull final Registry<K, BiConsumer<K, ? extends V>> consumerRegistry,
                  @Nullable Processor<Runnable, Runnable> processor,
                  int concurrency,
                  @Nullable final Router router,
                  @Nullable Consumer<Throwable> processorErrorHandler) {
        super(consumerRegistry,
              concurrency,
              router,
              processorErrorHandler);
        this.processor = processor;
        this.inDispatcherContext = new ThreadLocal<>();
        this.firehoseSubscription = new FirehoseSubscription();

        if (processor != null) {
            for (int i = 0; i < concurrency; i++) {
                processor.subscribe(Subscribers.unbounded(new BiConsumer<Runnable, SubscriptionWithContext<Void>>() {
                                                              @Override
                                                              public void accept(Runnable runnable, SubscriptionWithContext<Void> voidSubscriptionWithContext) {
                                                                  runnable.run();
                                                              }
                                                          }));
            }
            processor.onSubscribe(firehoseSubscription);
        }
    }

    @Override
    protected void accept(final K key, final V value) {
        // Backpressure
        while ((inDispatcherContext.get() == null || !inDispatcherContext.get()) &&
               !this.firehoseSubscription.maybeClaimSlot()) {
            try {
                //LockSupport.parkNanos(10000);
                Thread.sleep(10); // TODO: Migrate to parknanos
            } catch (InterruptedException e) {
                errorHandlerOrThrow(e);
            }
        }

        Boolean inContext = inDispatcherContext.get();
        if (inContext != null && inContext) {
            // Since we're already in the context, we should route syncronously
            try {
                route(key, value);
            } catch (Throwable outer) {
                errorHandlerOrThrow(outer);
            }
        } else {
            processor.onNext(new Runnable() {
                @Override
                public void run() {
                    try {
                        inDispatcherContext.set(true);
                        route(key, value);
                    } catch (Throwable outer) {
                        errorHandlerOrThrow(new RuntimeException("Exception in key: " + key.toString(), outer));
                    } finally {
                        inDispatcherContext.set(false);
                    }
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    protected void route(K key, V value) {
        List<Registration<K, ? extends BiConsumer<K, ? extends V>>> registrations = getConsumerRegistry().select(key);

        if (registrations.isEmpty()) {
            return;
        }

        if (registrations.get(0) instanceof DelayedRegistration) {
            getRouter().route(key, value, registrations, null, getProcessorErrorHandler());
            getRouter().route(key, value, getConsumerRegistry().select(key), null, getProcessorErrorHandler());
        } else {
            getRouter().route(key, value, registrations, null, getProcessorErrorHandler());
        }
    }

}
