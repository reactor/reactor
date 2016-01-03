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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.reactivestreams.Publisher;
import reactor.core.publisher.FluxFactory;
import reactor.core.subscriber.BlockingQueueSubscriber;
import reactor.core.support.ReactiveState;

/**
 * Create Reactive Streams Publishers from existing data, from custom callbacks (PublisherFactory) or from existing
 * Publishers (lift or combinatory operators).
 * @author Stephane Maldini
 * @since 2.5
 */
@Deprecated
public final class Publishers extends FluxFactory {

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> BlockingQueue<IN> toReadQueue(Publisher<IN> source) {
		int size = ReactiveState.SMALL_BUFFER_SIZE;
		return new BlockingQueueSubscriber<>(source, null,  new ArrayBlockingQueue<IN>(size), false, size);
	}

}
