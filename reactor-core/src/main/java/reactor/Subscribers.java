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
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.subscriber.EmptySubscriber;
import reactor.core.subscriber.SubscriberFactory;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;

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
		return EmptySubscriber.instance();
	}


	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toWriteQueue(Subscriber<IN> target) {
		return toWriteQueue(target, ReactiveState.SMALL_BUFFER_SIZE);
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
		subscriber.onSubscribe(EmptySubscription.INSTANCE);
		return subscriber;
	}

}
