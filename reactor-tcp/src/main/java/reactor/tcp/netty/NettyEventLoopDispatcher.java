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

package reactor.tcp.netty;

import io.netty.channel.EventLoop;
import reactor.fn.Event;
import reactor.fn.Supplier;
import reactor.fn.cache.Cache;
import reactor.fn.cache.LoadingCache;
import reactor.fn.dispatch.AbstractDispatcher;

/**
 * @author Jon Brisbin
 */
@SuppressWarnings({"rawtypes"})
public class NettyEventLoopDispatcher extends AbstractDispatcher {

	private final EventLoop   eventLoop;
	private final Cache<Task> readyTasks;

	public NettyEventLoopDispatcher(EventLoop eventLoop, int backlog) {
		this.eventLoop = eventLoop;
		this.readyTasks = new LoadingCache<Task>(
				new Supplier<Task>() {
					@Override
					public Task get() {
						return new NettyEventLoopTask();
					}
				},
				backlog,
				150L
		);
	}

	@Override
	public void shutdown() {
		eventLoop.shutdown();
		super.shutdown();
	}

	@Override
	public void halt() {
		eventLoop.shutdownNow();
		super.halt();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <E extends Event<?>> Task<E> createTask() {
		Task t = readyTasks.allocate();
		return (null != t ? t : new NettyEventLoopTask());
	}

	private final class NettyEventLoopTask extends Task<Event<Object>> implements Runnable {
		@Override
		public void submit() {
			eventLoop.execute(this);
		}

		@Override
		public void run() {
			try {
				execute();
			} finally {
				reset();
				readyTasks.deallocate(this);
			}
		}
	}

}
