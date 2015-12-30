/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.ReactorFatalException;
import reactor.core.processor.BaseProcessor;
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.subscriber.SubscriberFactory;
import reactor.core.support.SignalType;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class Subscribers extends SubscriberFactory {


	/**
	 * @param <IN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Subscriber<IN> empty() {
		return (EmptySubscriber<IN>) EMPTY;
	}


	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toWriteQueue(Subscriber<IN> target) {
		return toWriteQueue(target, BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toWriteQueue(Subscriber<IN> target, int size) {
		return toWriteQueue(target, size, new ArrayBlockingQueue<IN>(size));
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toWriteQueue(Subscriber<IN> target, int size, Queue<IN> store) {
		return new BlockingQueueSubscriber<>(null, target, store, size);
	}


	/**
	 *
	 * @param subscriber
	 * @param <I>
	 * @return
	 */
	public static <I, E extends Subscriber<I>> E start(E subscriber) {
		subscriber.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
		return subscriber;
	}

	private static final EmptySubscriber<?> EMPTY = new EmptySubscriber<>();

	private static class EmptySubscriber<IN> implements Subscriber<IN> {

		@Override
		public void onSubscribe(Subscription s) {
			//IGNORE
		}

		@Override
		public void onNext(IN in) {
			//IGNORE
		}

		@Override
		public void onError(Throwable t) {
			throw ReactorFatalException.create(t);
		}

		@Override
		public void onComplete() {
			//IGNORE
		}
	}

}
