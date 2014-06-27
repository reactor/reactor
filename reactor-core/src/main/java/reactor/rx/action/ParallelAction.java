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
import reactor.rx.StreamSubscription;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ParallelAction<O> extends Action<O, Action<O, O>> {

	private final ParallelStream[]      publishers;
	private final MultiThreadDispatcher multiDispatcher;
	private final int                   poolSize;

	@SuppressWarnings("unchecked")
	public ParallelAction(Dispatcher parentDispatcher, Dispatcher multiDispatcher,
	                      Integer poolSize, Integer batchSize) {
		super(parentDispatcher);

		Assert.state(MultiThreadDispatcher.class.isAssignableFrom(multiDispatcher.getClass()),
				"Given dispatcher [" + multiDispatcher + "] is not a MultiThreadDispatcher.");

		this.multiDispatcher = ((MultiThreadDispatcher) multiDispatcher);
		if (poolSize <= 0) {
			this.poolSize = this.multiDispatcher.getNumberThreads();
		} else {
			this.poolSize = poolSize;
		}
		this.publishers = new ParallelStream[this.poolSize];
		for (int i = 0; i < this.poolSize; i++) {
			this.publishers[i] = new ParallelStream<O>(this, batchSize);
		}
	}

	@Override
	public <A, E extends Action<Action<O, O>, A>> E connect(@Nonnull E stream) {
		E action = super.connect(stream);
		action.prefetch(poolSize).env(environment);
		return action;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected StreamSubscription<Action<O, O>> createSubscription(final Subscriber<Action<O, O>> subscriber) {
		return new StreamSubscription<Action<O, O>>(this, subscriber) {
			AtomicLong cursor = new AtomicLong();

			@Override
			public void request(int elements) {
				int i = 0;
				while (i < poolSize && i < cursor.get()) {
					i++;
				}

				while (i < elements && i < poolSize) {
					cursor.getAndIncrement();
						subscriber.onNext(publishers[i]);
					i++;
				}

				if(i == poolSize){
					subscriber.onComplete();
				}
			}
		};
	}

	public int getPoolSize() {
		return poolSize;
	}

	public ParallelStream[] getPublishers() {
		return publishers;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(final O ev) {
		reactor.function.Consumer<Void> completeHandler = new reactor.function.Consumer<Void>() {
			@Override
			public void accept(Void any) {
				int index = multiDispatcher.getThreadIndex() % poolSize;
				try {
					publishers[index].broadcastNext(ev);
				} catch (Throwable t) {
					publishers[index].broadcastError(t);
				}
			}
		};
		multiDispatcher.dispatch(this, null, null, null, ROUTER, completeHandler);
	}


	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		try {
			final AtomicInteger completed = new AtomicInteger(poolSize);
			reactor.function.Consumer<Throwable> dispatchErrorHandler = new reactor.function.Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					int i = multiDispatcher.getThreadIndex() % poolSize;
					completed.decrementAndGet();

					while (completed.get() > 0) {
						LockSupport.parkNanos(1);
					}

					publishers[i].broadcastError(throwable);
				}
			};
			for(int i = 0; i < poolSize; i++){
				multiDispatcher.dispatch(this, ev, null, null, ROUTER, dispatchErrorHandler);
			}
		} catch (Throwable dispatchError) {
			error = dispatchError;
		}
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		final AtomicInteger completed = new AtomicInteger(poolSize);
		reactor.function.Consumer<Void> completeHandler = new reactor.function.Consumer<Void>() {
			@Override
			public void accept(Void any) {
				int i = multiDispatcher.getThreadIndex() % poolSize;
				completed.decrementAndGet();

				while (completed.get() > 0) {
					LockSupport.parkNanos(1);
				}

				try {
					publishers[i].broadcastComplete();
				} catch (Throwable t) {
					publishers[i].broadcastError(t);
				}
			}
		};
		for(int i = 0; i < poolSize; i++){
			multiDispatcher.dispatch(this, null, null, null, ROUTER, completeHandler);
		}
	}

	@Override
	protected void request(final int n) {
		super.request(n);
	}

	static private class ParallelStream<O> extends Action<O, O> {
		final ParallelAction<O> parallelAction;

		private ParallelStream(ParallelAction<O> parallelAction, Integer batchSize) {
			super(parallelAction.multiDispatcher, batchSize / parallelAction.getPoolSize() - RESERVED_SLOTS);
			this.parallelAction = parallelAction;
		}

		@Override
		protected StreamSubscription<O> createSubscription(Subscriber<O> subscriber) {
			return new StreamSubscription<O>(this, subscriber) {
				@Override
				public void request(int elements) {
					super.request(elements);
					parallelAction.requestUpstream(capacity, buffer.isComplete(), elements);
				}
			};
		}
	}
}
