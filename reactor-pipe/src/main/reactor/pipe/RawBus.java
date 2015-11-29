package reactor.pipe;

import org.reactivestreams.Processor;
import reactor.Subscribers;
import reactor.bus.AbstractBus;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registry;
import reactor.bus.routing.Router;
import reactor.core.subscription.SubscriptionWithContext;
import reactor.core.support.SignalType;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.pipe.registry.DelayedRegistration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class RawBus<K, V> extends AbstractBus<K, V> {

  private final Processor<Runnable, Runnable> processor;

  public RawBus(@Nonnull final Registry<K, BiConsumer<K, ? extends V>> consumerRegistry,
                @Nullable Processor<Runnable, Runnable> processor,
      int concurrency,
                @Nullable final Router router,
                @Nullable Consumer<Throwable> processorErrorHandler,
                @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
    super(consumerRegistry,
          concurrency,
          router,
          processorErrorHandler,
          uncaughtErrorHandler);
    this.processor = processor;

    if (processor != null) {
      for (int i = 0; i < concurrency; i++) {
        processor.subscribe(Subscribers.unbounded(new BiConsumer<Runnable, SubscriptionWithContext<Void>>() {
                                                    @Override
                                                    public void accept(Runnable runnable, SubscriptionWithContext<Void> voidSubscriptionWithContext) {
                                                        runnable.run();
                                                    }
        },
            uncaughtErrorHandler));
      }
      processor.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
    }
  }

  @Override
  protected void accept(final K key, final V value) {
    if (processor == null) {
      try {
        route(key, value);
      } catch (Throwable throwable) {
        errorHandlerOrThrow(throwable);
      }
    } else {
      processor.onNext(new Runnable() {
        @Override
        public void run() {
          route(key, value);
        }
      });
    }
  }

  protected void route(K key, V value) {
    {
      List<Registration<K, ? extends BiConsumer<K, ? extends V>>> registrations = getConsumerRegistry().select(key);
      if (registrations.isEmpty()) {
        return;
      } else if (registrations.get(0) instanceof DelayedRegistration) {
        getRouter().route(key, value, registrations, null, getProcessorErrorHandler());
        getRouter().route(key, value, getConsumerRegistry().select(key), null, getProcessorErrorHandler());
      } else {
        getRouter().route(key, value, registrations, null, getProcessorErrorHandler());
      }
    }

  }

}
