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
package reactor.io.net.impl.netty.internal;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.PlatformDependent;
import reactor.io.net.impl.netty.internal.EpollDetector;

/**
 * @author Stephane Maldini
 */
public class NettyNativeDetector {

	private final static boolean epoll;


	static {
		if (!PlatformDependent.isWindows()
				&&
				Boolean.parseBoolean(System.getProperty("reactor.io.epoll", "true"))) {
				epoll = EpollDetector.hasEpoll();
		}
		else {
			epoll = true;
		}
	}

	public static EventLoopGroup newEventLoopGroup(int threads, ThreadFactory factory) {
		return epoll ? EpollDetector.newEventLoopGroup(threads, factory) : new NioEventLoopGroup(threads, factory);
	}

	public static Class<? extends ServerChannel> getServerChannel(EventLoopGroup group) {
		return epoll ? EpollDetector.getServerChannel(group) :  NioServerSocketChannel.class;
	}

	public static Class<? extends Channel> getChannel(EventLoopGroup group) {
		return epoll ? EpollDetector.getChannel(group) :  NioSocketChannel.class;
	}

	public static Class<? extends Channel> getDatagramChannel(EventLoopGroup group) {
		return epoll ? EpollDetector.getDatagramChannel(group) :  NioDatagramChannel.class;
	}

}
