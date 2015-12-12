/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.bus;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.bus.filter.PassThroughFilter;
import reactor.bus.publisher.BusPublisher;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registry;
import reactor.bus.routing.ConsumerFilteringRouter;
import reactor.bus.routing.Router;
import reactor.bus.selector.Selector;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.support.Assert;
import reactor.core.support.UUIDUtils;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * A reactor is an event gateway that allows other components to register {@link Event} {@link Consumer}s that can
 * subsequently be notified of events. A consumer is typically registered with a {@link Selector} which, by matching on
 * the notification key, governs which events the consumer will receive. </p> When a {@literal Reactor} is notified of
 * an {@link Event}, a task is dispatched using the reactor's {@link Processor} which causes it to be executed
 * on a
 * thread based on the implementation of the {@link Processor} being used.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 * @author Alex Petrov
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class AbstractBus<K, V> implements Bus<K, V> {

  protected static final Router DEFAULT_EVENT_ROUTER = new ConsumerFilteringRouter(
    new PassThroughFilter()
  );

  private final Registry<K, BiConsumer<K, ? extends V>> consumerRegistry;
  private final Router                                  router;
  private final Consumer<Throwable>                     processorErrorHandler;
  private final int                                     concurrency;

  private volatile UUID id;

  /**
   * Create a new {@literal Reactor} that uses the given {@code processor} and {@code eventRouter}.
   *
   * @param consumerRegistry      The {@link Registry} to be used to match {@link Selector} and dispatch to {@link
   *                              Consumer}
   * @param concurrency           The allowed number of concurrent routing. This is highly dependent on the
   *                              processor used. Only "Work" processors like {@link reactor.core.processor
   *                              .RingBufferWorkProcessor} will be meaningful as they distribute their messages,
   *                              default RS behavior is to broadcast resulting in a matching number of duplicate
   *                              routing.
   * @param router                The {@link Router} used to route events to {@link Consumer Consumers}. May be {@code
   *                              null} in which case the default event router that broadcasts events to all of the
   *                              registered consumers that {@link Selector#matches(Object) match} the notification
   *                              key and does not perform any type
   *                              conversion will be used.
   * @param processorErrorHandler The {@link Consumer} to be used on {@link Processor} exceptions. May be {@code null}
   *                              in which case exceptions will be logged.
   */
  @SuppressWarnings("unchecked")
  public AbstractBus(@Nonnull Registry<K, BiConsumer<K, ? extends V>> consumerRegistry,
                     int concurrency,
                     @Nullable Router router,
                     @Nullable Consumer<Throwable> processorErrorHandler) {
    Assert.notNull(consumerRegistry, "Consumer Registry cannot be null.");
    this.consumerRegistry = consumerRegistry;
    this.concurrency = concurrency;
    this.router = (null == router ? DEFAULT_EVENT_ROUTER : router);
    if (null == processorErrorHandler) {
      this.processorErrorHandler = new Consumer<Throwable>() {
        final Logger log = LoggerFactory.getLogger(AbstractBus.class);
        @Override
        public void accept(Throwable t) {
          log.error(t.getMessage(), t);
        }
      };
    } else {
      this.processorErrorHandler = processorErrorHandler;
    }
  }

  /**
   * Get the unique, time-used {@link UUID} of this {@literal Reactor}.
   *
   * @return The {@link UUID} of this {@literal Reactor}.
   */
  public synchronized UUID getId() {
    if (null == id) {
      id = UUIDUtils.create();
    }
    return id;
  }

  /**
   * Get the {@link Registry} is use to maintain the {@link Consumer}s currently listening for events on this
   * {@literal
   * Reactor}.
   *
   * @return The {@link Registry} in use.
   */
  public Registry<K, BiConsumer<K, ? extends V>> getConsumerRegistry() {
    return consumerRegistry;
  }

  /**
   * Get the {@link Router} used to route events to {@link Consumer Consumers}.
   *
   * @return The {@link Router}.
   */
  public Router getRouter() {
    return router;
  }

  /**
   * Get the {@link Consumer<Throwable>} processor error handler
   *
   * @return The {@link Consumer<Throwable>} processor error handler in use
   */
  public Consumer<Throwable> getProcessorErrorHandler() {
    return processorErrorHandler;
  }

  @Override
  public boolean respondsToKey(K key) {
    List<Registration<K, ? extends BiConsumer<K, ? extends V>>> registrations = consumerRegistry.select(key);

    if (registrations.isEmpty())
      return false;

    for (Registration<?, ?> reg : registrations) {
      if (!reg.isCancelled()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public <T extends V> Registration<K, BiConsumer<K, ? extends V>> on(final Selector selector,
                                                                      final BiConsumer<K, T> consumer) {
    Assert.notNull(selector, "Selector cannot be null.");
    Assert.notNull(consumer, "Consumer cannot be null.");

    return consumerRegistry.register(selector, consumer);
  }

  @Override
  public <V1 extends V> Registration<K, BiConsumer<K, ? extends V>> on(final Selector selector,
                                                                       final Consumer<V1> consumer) {
    return on(selector, new BiConsumer<K, V1>() {
      @Override
      public void accept(K k, V1 v) {
        consumer.accept(v);
      }
    });
  }

  @Override
  public <V1 extends V> Registration<K, BiConsumer<K, ? extends V>> onKey(final K key,
                                                                          final BiConsumer<K, V1> consumer) {
    Assert.notNull(key, "Key cannot be null.");
    Assert.notNull(consumer, "Consumer cannot be null.");

    return consumerRegistry.register(key, consumer);
  }

  @Override
  public <T extends V> Registration<K, BiConsumer<K, ? extends V>> onKey(final K key,
                                                                         final Consumer<T> consumer) {
    return onKey(key, new BiConsumer<K, T>() {
      @Override
      public void accept(K k, T v) {
        consumer.accept(v);
      }
    });
  }

  /**
   * Concurrency level of the Bus
   *
   * @return concurrency level
   */
  public int getConcurrency() {
    return concurrency;
  }

  /**
   * Attach a Publisher to the {@link Bus} with the specified {@link Selector}.
   *
   * @param broadcastSelector the {@link Selector}/{@literal Object} tuple to listen to
   * @return a new {@link Publisher}
   * @since 2.0
   */
  public Publisher<? extends V> on(Selector broadcastSelector) {
    return new BusPublisher<>(this, broadcastSelector);
  }

  @Override
  public AbstractBus notify(final K key, final V value) {
    Assert.notNull(key, "Key cannot be null.");
    Assert.notNull(value, "Event cannot be null.");

    accept(key, value);

    return this;
  }

  @Override
  public AbstractBus notify(K key, Supplier<? extends V> supplier) {
    return notify(key, supplier.get());
  }

  protected void errorHandlerOrThrow(Throwable t) {
    if (processorErrorHandler != null) {
      Exceptions.throwIfFatal(t);
      processorErrorHandler.accept(t);
    } else {
      throw ReactorFatalException.create(t);
    }
  }

  protected abstract void accept(K key, V value);

  protected void route(K key, V value) {
    router.route(key, value, consumerRegistry.select(key), null, processorErrorHandler);
  }

}
