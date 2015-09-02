/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.processor.BaseProcessor;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.subscriber.SubscriberFactory;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Subscribers extends SubscriberFactory {

	/**
	 *
	 * @param source
	 * @param <T>
	 * @return
	 */
	public static <T> Subscriber<T> flowControl(Subscriber<T> source) {
		return source;
	}


	/**
	 *
	 * @param source
	 * @param <T>
	 * @return
	 */
	public static <T> Subscriber<T> group(Subscriber<T> source) {
		return source;
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> writeQueue(Subscriber<IN> target) {
		return writeQueue(target, BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> writeQueue(Subscriber<IN> target, int size) {
		return writeQueue(target, size, new ArrayBlockingQueue<IN>(size));
	}

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> writeQueue(Subscriber<IN> target, int size, Queue<IN> store) {
		return new BlockingQueueSubscriber<>(null, target, store, size);
	}


	/**
	 *
	 * @param subscriber
	 * @param <I>
	 * @return
	 */
	public static <I, E extends Subscriber<I>> E start(E subscriber) {
		subscriber.onSubscribe(BaseSubscriber.NOOP_SUBSCRIPTION);
		return subscriber;
	}
}
