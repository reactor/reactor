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
package reactor.rx.stream;

import org.reactivestreams.Subscriber;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;
import reactor.rx.action.ParallelAction;

/**
 * A stream emitted from the {@link reactor.rx.Stream#parallel()} action that tracks its position in the hosting
 * {@link reactor.rx.action.ParallelAction}. It also retains the last time it requested anything for
 * latency monitoring {@link ParallelAction#monitorLatency(long)}.
 * The Stream will complete or fail whever the parent parallel action terminates itself or when broadcastXXX is called.
 *
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.parallel(8).consume(parallelStream -> parallelStream.consume())
 * }
 *
 * @author Stephane Maldini
*/
public class ParallelStream<O> extends Stream<O> {
	private final ParallelAction<O> parallelAction;
	private final int               index;

	private volatile long lastRequestedTime = -1l;

	public ParallelStream(ParallelAction<O> parallelAction, Dispatcher dispatcher, int index) {
		super(dispatcher);
		this.parallelAction = parallelAction;
		this.index = index;
	}

	public long getLastRequestedTime() {
		return lastRequestedTime;
	}

	public int getIndex() {
		return index;
	}

	@Override
	public void broadcastComplete() {
		dispatch(new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				ParallelStream.super.broadcastComplete();
			}
		});
	}

	@Override
	public void broadcastError(Throwable throwable) {
		dispatch(throwable, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				ParallelStream.super.broadcastError(throwable);
			}
		});
	}

	@Override
	protected StreamSubscription<O> createSubscription(Subscriber<? super O> subscriber, boolean reactivePull) {
		return new StreamSubscription<O>(this, subscriber) {
			@Override
			public void request(long elements) {
				super.request(elements);
				lastRequestedTime = System.currentTimeMillis();
				parallelAction.parallelRequest(elements, index);
			}

			@Override
			public void cancel() {
				super.cancel();
				parallelAction.clean(index);
			}

		};
	}

	@Override
	public String toString() {
		return super.toString() + "{" + (index + 1) + "/" + parallelAction.getPoolSize() + "}";
	}
}
