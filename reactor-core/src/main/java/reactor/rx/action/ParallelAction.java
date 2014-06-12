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

import org.reactivestreams.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.MultiThreadDispatcher;
import reactor.function.Supplier;
import reactor.rx.StreamSubscription;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ParallelAction<O, E extends Pipeline<O>> extends Action<O, E> {

	private final E[]                   publishers;
	private final MultiThreadDispatcher dispatcher;
	private final int                   poolSize;

	@SuppressWarnings("unchecked")
	public ParallelAction(Dispatcher parentDispatcher, Dispatcher dispatcher,
	                      Integer poolSize, Supplier<E> pipelineProvider) {
		super(parentDispatcher);

		Assert.state(MultiThreadDispatcher.class.isAssignableFrom(dispatcher.getClass()),
				"Given dispatcher [" + dispatcher + "] is not a MultiThreadDispatcher.");

		this.dispatcher =  ((MultiThreadDispatcher) dispatcher);
		if (poolSize <= 0) {
			this.poolSize = this.dispatcher.getNumberThreads();
		} else {
			this.poolSize = poolSize;
		}
		this.publishers = (E[]) new Pipeline[this.poolSize];
		for (int i = 0; i < this.poolSize; i++) {
			this.publishers[i] = pipelineProvider.get();
		}
	}

	@Override
	public <A, E1 extends Action<E, A>> E1 connect(@Nonnull E1 stream) {
		E1 action = super.connect(stream);
		action.prefetch(poolSize);
		return action;
	}

	@Override
	protected StreamSubscription<E> createSubscription(Subscriber<E> subscriber) {
			return new StreamSubscription<E>(this, subscriber) {
				AtomicLong cursor = new AtomicLong();

				@Override
				public void request(int elements) {
					super.request(elements);

					int i = 0;
					while (i < poolSize && i < cursor.get()) {
						i++;
					}

					while (i < elements && i < poolSize) {
						cursor.getAndIncrement();
						onNext(publishers[i]);
						i++;
					}

					if(i >= poolSize){
						requestUpstream(capacity, buffer.isComplete(), elements);
						onComplete();
					}
				}
			};
	}

	@Override
	public void onNext(O ev) {
		this.dispatcher.dispatch(this, ev, null, null, ROUTER, this);
	}

	public int getPoolSize() {
		return poolSize;
	}

	public E[] getPublishers() {
		return publishers;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(O ev) {
		int index = dispatcher.getThreadIndex() % poolSize;
		publishers[index].broadcastNext(ev);
	}

	@Override
	protected void doError(Throwable ev) {
		for (int i = 0; i < poolSize; i++) {
			publishers[i].broadcastError(ev);
		}
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		for (int i = 0; i < poolSize; i++) {
			publishers[i].broadcastFlush();
		}
		super.broadcastComplete();
	}
}
