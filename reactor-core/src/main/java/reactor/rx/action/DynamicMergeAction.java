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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class DynamicMergeAction<E, O> extends Action<Publisher<E>, O> {

	private final FanInAction<E, O, ? extends FanInAction.InnerSubscriber<E, O>> fanInAction;

	@SuppressWarnings("unchecked")
	public DynamicMergeAction(
			Dispatcher dispatcher
	) {
		this(dispatcher, (FanInAction<E, O, ? extends FanInAction.InnerSubscriber<E, O>>) new MergeAction<O>(dispatcher));
	}

	public DynamicMergeAction(
			Dispatcher dispatcher,
			FanInAction<E, O, ? extends FanInAction.InnerSubscriber<E, O>> fanInAction
	) {
		super(dispatcher);
		this.fanInAction = fanInAction;
		fanInAction.dynamicMergeAction = this;
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		fanInAction.subscribe(subscriber);
	}

	@Override
	protected void doNext(Publisher<E> value) {
		fanInAction.addPublisher(value);
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		if (fanInAction.runningComposables.get() == 0) {
			fanInAction.innerSubscriptions.onComplete();
		}
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		fanInAction.doError(ev);
	}

	@Override
	public Action<Publisher<E>, O> capacity(long elements) {
		fanInAction.capacity(elements);
		return super.capacity(elements);
	}

	@Override
	public Action<Publisher<E>, O> keepAlive(boolean keepAlive) {
		fanInAction.keepAlive(keepAlive);
		return super.keepAlive(false);
	}

	@Override
	public Action<Publisher<E>, O> env(Environment environment) {
		fanInAction.env(environment);
		return super.env(environment);
	}

	@Override
	public Action<Publisher<E>, O> resume() {
		fanInAction.resume();
		return super.resume();
	}

	@Override
	public Action<Publisher<E>, O> pause() {
		fanInAction.pause();
		return super.pause();
	}

	@Override
	public Action<Publisher<E>, O> cancel() {
		fanInAction.cancel();
		return super.cancel();
	}

	public FanInAction<E, O, ?> mergedStream() {
		return fanInAction;
	}

}
