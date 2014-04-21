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

import org.reactivestreams.api.Processor;
import org.reactivestreams.spi.Subscriber;
import org.reactivestreams.spi.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.Router;
import reactor.filter.PassThroughFilter;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;
import reactor.rx.StreamUtils;
import reactor.rx.action.support.ActionException;
import reactor.timer.Timer;
import reactor.util.Assert;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class Action<I, O> extends Stream<O> implements Processor<I, O>, Consumer<I>, Subscriber<I>, Flushable {

	private static final Router ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	private Subscription subscription;

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
	public Stream<O> timeout(long timeout) {
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
	public Stream<O> timeout(long timeout, Timer timer) {
		Stream<?> composable = subscription != null &&
				StreamSubscription.class.isAssignableFrom(subscription.getClass()) ?
				((StreamSubscription<O>) subscription).getPublisher() :
				this;

		final TimeoutAction<O> d = new TimeoutAction<O>(
				getDispatcher(),
				composable,
				timer,
				timeout
		);
		connect(d);

		return this;
	}

	@Override
	public void accept(I i) {
		try {
			doNext(i);
		} catch (Throwable cause) {
			doError(cause);
		}
	}

	@Override
	public void onNext(I ev) {
		if (error != null) throw ActionException.INSTANCE;
		dispatcher.dispatch(this, null, null, null, ROUTER, this);
	}

	@Override
	public void onFlush() {
		if (error != null) throw ActionException.INSTANCE;
		doFlush();
	}

	@Override
	public void onComplete() {
		if (error != null) throw ActionException.INSTANCE;
		reactor.function.Consumer<Void> completeHandler = new reactor.function.Consumer<Void>() {
			@Override
			public void accept(Void any) {
				try {
					doComplete();
				} catch (Throwable t) {
					doError(t);
				}
			}
		};
		dispatcher.dispatch(this, null, null, null, ROUTER, completeHandler);

	}

	@Override
	public void onError(Throwable cause) {
		if (error != null) throw ActionException.INSTANCE;
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
		if (this.subscription != null)
			throw new IllegalArgumentException("Already has an active subscription");

		this.subscription = subscription;
		reactor.function.Consumer<Subscription> subscriptionHandler = new reactor.function.Consumer<Subscription>() {
			@Override
			public void accept(Subscription sub) {
				try {
					doSubscribe(sub);
				} catch (Throwable t) {
					doError(t);
				}
			}
		};
		dispatcher.dispatch(this, subscription, null, null, ROUTER, subscriptionHandler);
	}

	/**
	 * Flush any cached or unprocessed values through this oldest kwown {@link Stream} ancestor.
	 */
	@Override
	public void flush() {
		findOldestStream().broadcastFlush();
	}

	@Override
	public Stream<O> cancel() {
		subscription.cancel();
		return super.cancel();
	}

	@Override
	public Stream<O> pause() {
		pause = true;
		return super.pause();
	}

	@Override
	public Stream<O> resume() {
		pause = false;
		available();
		return super.resume();
	}

	@Override
	public String debug() {
		return StreamUtils.browse(findOldestStream());
	}

	protected void available() {
		if (batchSize != 0 && !pause) {
			subscription.requestMore(batchSize > 0 ? batchSize : 1);
		}
	}

	protected void doFlush() {
		broadcastFlush();
	}

	protected void doSubscribe(Subscription subscription) {
		if (null != this.subscription) {
			this.subscription = subscription;
		}
		available();
	}

	protected void doComplete() {
		broadcastComplete();
		cancel();
	}

	protected void doNext(I ev) {
	}

	protected void doError(Throwable ev) {
		broadcastError(ev);
	}

	private Stream<?> findOldestStream() {
		Action<?, ?> that = this;

		while (that.subscription != null
				&& StreamSubscription.class.isAssignableFrom(that.subscription.getClass())
				&& Action.class.isAssignableFrom(((StreamSubscription<?>) that.subscription).getPublisher().getClass())
				) {
			that = (Action<?,?>)((StreamSubscription<?>) that.subscription).getPublisher();
		}
		return that;
	}

	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	public Subscriber<I> getSubscriber() {
		return this;
	}
}
