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
package reactor.core.composable.action;

import org.reactivestreams.api.Consumer;
import org.reactivestreams.spi.Subscriber;
import org.reactivestreams.spi.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.event.lifecycle.Pausable;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.Router;
import reactor.filter.PassThroughFilter;

/**
 * Base class for all Composable actions such as map, reduce, filter...
 * An Action can be implemented to register against specific callbacks (doNext,doComplete,doError,doSubscribe).
 * It is an asynchronous boundary and will run the callbacks using the input {@link Dispatcher}. Actions can also
 * produce a result and will offer cascading over {@link this#output} if defined.
 *
 * @param <T> The type of the input values
 * @param <V> The type of the output values
 * @author Stephane Maldini
 * @since 1.1
 */
public abstract class Action<T, V> implements Consumer<T>, Subscriber<T>, reactor.function.Consumer<T>, Pausable {

	private static final Router ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	protected final ActionProcessor<V> output;
	protected final Dispatcher         dispatcher;

	protected int prefetch = 0;

	private Subscription subscription;

	Throwable error;
	boolean pause = false;

	public Action(Dispatcher dispatcher) {
		this(dispatcher, null);
	}

	public Action(Dispatcher dispatcher, ActionProcessor<V> output) {
		this.output = output;
		this.dispatcher = dispatcher;
	}

	protected void available(){
		if(prefetch > 0 && !pause){
			subscription.requestMore(prefetch);
		}
	}

	protected void doNext(T ev) {
	}

	protected void doSubscribe(Subscription subscription) {
		if(null != this.subscription){
			this.subscription = subscription;
		}
		available();
	}

	protected void doComplete() {
		if (output != null) {
			output.onComplete();
		}
		cancel();
	}

	protected void doError(Throwable ev) {
		if (output != null) {
			output.onError(ev);
		}
	}

	@Override
	public void accept(T t) {
		try {
			doNext(t);
		} catch (Throwable cause) {
			doError(cause);
		}
	}

	@Override
	public Subscriber<T> getSubscriber() {
		return this;
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

	@Override
	public void onNext(T ev) {
		if(error != null) throw ActionException.INSTANCE;
		dispatcher.dispatch(this, ev, null, null, ROUTER, this);
	}

	@Override
	public void onError(Throwable cause) {
		if(error != null) throw ActionException.INSTANCE;
		try{
			error = cause;
			reactor.function.Consumer<Throwable> dispatchErrorHandler = new reactor.function.Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					doError(throwable);
				}
			};
			dispatcher.dispatch(this, cause, null, null, ROUTER, dispatchErrorHandler);
		}catch (Throwable dispatchError){
			error = dispatchError;
		}
	}

	@Override
	public void onComplete() {
		if(error != null) throw ActionException.INSTANCE;
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
	public Action<T,V> cancel() {
		subscription.cancel();
		return this;
	}

	@Override
	public Action<T,V> pause() {
		pause = true;
		return this;
	}

	@Override
	public Pausable resume() {
		pause = false;
		available();
		return this;
	}

	public Action<T,V> prefetch(int elements){
		this.prefetch = elements;
		return this;
	}

	public Subscription getSubscription() {
		return subscription;
	}

	public final ActionProcessor<V> getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "{" +
				"prefetch=" + prefetch +
				'}';
	}
}
