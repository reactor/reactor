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

package reactor.event.dispatch;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.function.Consumer;

import java.util.concurrent.BlockingQueue;

/**
 * Implementation of {@link Dispatcher} that uses a {@link BlockingQueue} to queue tasks to be executed.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
@Deprecated
public final class EventLoopDispatcher extends RingBufferDispatcher {

	/**
	 * Creates a new {@literal EventLoopDispatcher} with the given {@literal name} and {@literal backlog}.
	 *
	 * @param name
	 * 		The name
	 * @param backlog
	 * 		The backlog size
	 */
	public EventLoopDispatcher(String name, int backlog) {
		this(name, backlog, null);
	}

	/**
	 * Creates a new {@literal EventLoopDispatcher} with the given {@literal name} and {@literal backlog}.
	 *
	 * @param name
	 * 		The name
	 * @param backlog
	 * 		The backlog size
	 * @param uncaughtExceptionHandler
	 * 		The {@code UncaughtExceptionHandler}
	 */
	public EventLoopDispatcher(final String name,
	                           int backlog,
	                           final Consumer<Throwable> uncaughtExceptionHandler) {
		super(name, backlog, uncaughtExceptionHandler, ProducerType.MULTI, new BlockingWaitStrategy());
	}

}
