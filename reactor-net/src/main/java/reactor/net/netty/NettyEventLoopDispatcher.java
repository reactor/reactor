/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.net.netty;

import io.netty.channel.EventLoop;
import reactor.event.dispatch.MultiThreadDispatcher;

import java.util.concurrent.TimeUnit;

/**
 * A {@code Dispatcher} that runs tasks on a Netty {@link EventLoop}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings({"rawtypes"})
public class NettyEventLoopDispatcher extends MultiThreadDispatcher {

	private final EventLoop eventLoop;

	/**
	 * Creates a new Netty event loop-based dispatcher that will run tasks on the given {@code eventLoop} with the given
	 * {@code backlog} size.
	 *
	 * @param eventLoop
	 * 		The event loop to run tasks on
	 * @param backlog
	 * 		The size of the backlog of unexecuted tasks
	 */
	public NettyEventLoopDispatcher(EventLoop eventLoop, int backlog) {
		super(1, backlog);
		this.eventLoop = eventLoop;
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
	public void forceShutdown() {
		eventLoop.shutdownGracefully();
		super.forceShutdown();
	}

	@Override
	protected void execute(Task task) {
		eventLoop.execute(task);
	}

	@Override
	public void execute(Runnable command) {
		eventLoop.execute(command);
	}

	@Override
	public long remainingSlots() {
		return Long.MAX_VALUE;
	}

	@Override
	public int backlogSize() {
		return Integer.MAX_VALUE;
	}
}
