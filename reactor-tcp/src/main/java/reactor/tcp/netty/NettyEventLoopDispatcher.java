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
import reactor.pool.Pool;
import reactor.pool.LoadingPool;
import reactor.event.Event;
import reactor.event.dispatch.BaseLifecycleDispatcher;
import reactor.function.Supplier;

import java.util.concurrent.TimeUnit;

/**
 * A {@code Dispatcher} that runs tasks on a Netty {@link EventLoop}.
 *
 * @author Jon Brisbin
 */
@SuppressWarnings({"rawtypes"})
public class NettyEventLoopDispatcher extends BaseLifecycleDispatcher {

	private final EventLoop  eventLoop;
	private final Pool<Task> readyTasks;

	/**
	 * Creates a new Netty event loop-based dispatcher that will run tasks on the given {@code eventLoop} with the given
	 * {@code backlog} size.
	 *
	 * @param eventLoop The event loop to run tasks on
	 * @param backlog   The size of the backlog of unexecuted tasks
	 */
	public NettyEventLoopDispatcher(EventLoop eventLoop, int backlog) {
		this.eventLoop = eventLoop;
		this.readyTasks = new LoadingPool<Task>(
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
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		shutdown();
		try {
			return eventLoop.awaitTermination(timeout, timeUnit);
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return false;
	}

	@Override
	public void shutdown() {
		eventLoop.shutdownGracefully();
		super.shutdown();
	}

	@Override
	public void halt() {
		eventLoop.shutdownGracefully();
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
