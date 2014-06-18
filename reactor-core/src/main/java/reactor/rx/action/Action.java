/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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
package reactor.rx.action;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.Router;
import reactor.filter.PassThroughFilter;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;
import reactor.rx.StreamUtils;
import reactor.timer.Timer;
import reactor.util.Assert;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class Action<I, O> extends Stream<O> implements Processor<I, O>, Consumer<I> {

	protected static final Router ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	private static final Logger log = LoggerFactory.getLogger(Action.class);

	private final AtomicLong pendingRequest = new AtomicLong();

	private Subscription subscription;

	public static <O> Action<O, O> passthrough() {
		return passthrough(SynchronousDispatcher.INSTANCE);
	}

	public static <O> Action<O, O> passthrough(Dispatcher dispatcher) {
		return new Action<O, O>(dispatcher) {
			@Override
			protected void doNext(O ev) {
				broadcastNext(ev);
			}
		};
	}

	public Action() {
		super();
	}

	public Action(Dispatcher dispatcher) {
		super(dispatcher);
	}

	public Action(Dispatcher dispatcher, int batchSize) {
		super(dispatcher, batchSize);
	}

	/**
	 * Flush the parent if any or the current composable otherwise when the last notification occurred before {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @return this {@link Stream}
	 * @since 1.1
	 */
	public Action<O, O> timeout(long timeout) {
		Assert.state(getEnvironment() != null, "Cannot use default timer as no environment has been provided to this " +
				"Stream");
		return timeout(timeout, getEnvironment().getRootTimer());
	}

	/**
	 * Flush the parent if any or the current composable otherwise when the last notification occurred before {@param
	 * timeout} milliseconds. Timeout is run on the provided {@param timer}.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @param timer   the reactor timer to run the timeout on
	 * @return this {@link Stream}
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public Action<O, O> timeout(long timeout, Timer timer) {
		final TimeoutAction<O> d = new TimeoutAction<O>(
				getDispatcher(),
				timer,
				timeout
		);
		return connect(d);
	}

	public void available() {
		if (subscription != null && !pause) {
			request(batchSize);
		}
	}

	protected void request(final int n) {
		pendingRequest.getAndAdd(n);

		reactor.function.Consumer<Void> completeHandler = new reactor.function.Consumer<Void>() {
			@Override
			public void accept(Void any) {
				try {
					subscription.request(n);
				} catch (Throwable t) {
					doError(t);
				}
			}
		};
		dispatcher.dispatch(this, null, null, null, ROUTER, completeHandler);
	}

	protected void requestUpstream(AtomicLong capacity, boolean terminated, int elements) {
		if (subscription != null && !terminated) {
			int currentCapacity = capacity.intValue();
			if (!pause && currentCapacity > 0) {
				final int remaining = currentCapacity > elements ? elements : currentCapacity;
				request(remaining);
			}
		}
	}

	@Override
	protected StreamSubscription<O> createSubscription(final Subscriber<O> subscriber) {
		return new StreamSubscription<O>(this, subscriber) {
			@Override
			public void request(int elements) {
				super.request(elements);
				requestUpstream(capacity, buffer.isComplete(), elements);
			}
		};
	}

	@Override
	public void accept(I i) {
		try {
			pendingRequest.decrementAndGet();
			doNext(i);
		} catch (Throwable cause) {
			doError(cause);
		}
	}

	@Override
	public void onNext(I ev) {
		dispatcher.dispatch(this, ev, null, null, ROUTER, this);
	}

	@Override
	public void onComplete() {
		reactor.function.Consumer<Void> completeHandler = new reactor.function.Consumer<Void>() {
			@Override
			public void accept(Void any) {
				try {
					doComplete();
					/*if(!keepAlive){
						cancel();
					}*/
				} catch (Throwable t) {
					doError(t);
				}
			}
		};
		dispatcher.dispatch(this, null, null, null, ROUTER, completeHandler);

	}

	@Override
	public void onError(Throwable cause) {
		try {
			error = cause;
			reactor.function.Consumer<Throwable> dispatchErrorHandler = new reactor.function.Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					doError(throwable);
				}
			};
			dispatcher.dispatch(this, cause, null, null, ROUTER, dispatchErrorHandler);
		} catch (Throwable dispatchError) {
			error = dispatchError;
		}
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		if (this.subscription == null) {
			this.subscription = subscription;
			reactor.function.Consumer<Subscription> completeHandler = new reactor.function.Consumer<Subscription>() {
				@Override
				public void accept(Subscription subscription) {
					try {
						doSubscribe(subscription);
					} catch (Throwable t) {
						doError(t);
					}
				}
			};
			dispatcher.dispatch(this, subscription, null, null, ROUTER, completeHandler);
		}
	}

	@Override
	protected void removeSubscription(StreamSubscription<O> sub) {
		super.removeSubscription(sub);
		if (getState() == State.SHUTDOWN && subscription != null) {
			subscription.cancel();
		}
	}

	@Override
	public Action<I, O> cancel() {
		if (subscription != null)
			subscription.cancel();
		super.cancel();
		return this;
	}

	@Override
	public Action<I, O> pause() {
		super.pause();
		return this;
	}

	@Override
	public Action<I, O> resume() {
		super.resume();
		available();
		return this;
	}

	@Override
	public String debug() {
		return StreamUtils.browse(findOldestStream(false));
	}

	public <E> Processor<E, O> combine() {
		return combine(false);
	}

	@SuppressWarnings("unchecked")
	public <E> Action<E, O> combine(boolean reuse) {
		final Action<E, O> subscriber = (Action<E, O>) findOldestStream(reuse);
		final Stream<O> publisher = this;

		return new CombineAction<E, O>(publisher, subscriber);
	}

	@Override
	@SuppressWarnings("unchecked")

	public Action<I, O> prefetch(int elements) {
		return (Action<I, O>) super.prefetch(elements);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Action<I, O> env(Environment environment) {
		return (Action<I, O>) super.env(environment);
	}

	public Action<?, ?> findOldestStream(boolean resetState) {
		Action<?, ?> that = this;

		if (resetState) {
			resetState(that);
		}

		while (that.subscription != null
				&& StreamSubscription.class.isAssignableFrom(that.subscription.getClass())
				&& Action.class.isAssignableFrom(((StreamSubscription<?>) that.subscription).getPublisher().getClass())
				) {

			that = (Action<?, ?>) ((StreamSubscription<?>) that.subscription).getPublisher();

			if (resetState) {
				resetState(that);
			}

		}
		return that;
	}

	protected void doSubscribe(Subscription subscription) {
	}

	protected void doComplete() {
		broadcastComplete();
	}

	protected void doNext(I ev) {
	}

	protected void doError(Throwable ev) {
		broadcastError(ev);
	}

	protected void resetState(Action<?, ?> action) {
		action.setState(State.READY);
		error = null;
	}

	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	@SuppressWarnings("unchecked")
	public String toString() {
		return "{" +
				"state=" + getState() +
				", prefetch=" + getBatchSize() +
				(subscription != null &&
						StreamSubscription.class.isAssignableFrom(subscription.getClass()) ?
						", buffered=" + ((StreamSubscription<O>) subscription).getBufferSize() +
								", capacity=" + ((StreamSubscription<O>) subscription).getCapacity()
						: ", subscription="+subscription
				) + '}';
	}

}
