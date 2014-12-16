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

package reactor.io.net.netty;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import reactor.fn.Consumer;
import reactor.io.net.config.ClientSocketOptions;

/**
 * @author Jon Brisbin
 */
public class NettyClientSocketOptions extends ClientSocketOptions {

	private Consumer<ChannelPipeline> pipelineConfigurer;
	private NioEventLoopGroup         eventLoopGroup;

	public Consumer<ChannelPipeline> pipelineConfigurer() {
		return pipelineConfigurer;
	}

	public NettyClientSocketOptions pipelineConfigurer(Consumer<ChannelPipeline> pipelineConfigurer) {
		this.pipelineConfigurer = pipelineConfigurer;
		return this;
	}

	public NioEventLoopGroup eventLoopGroup() {
		return eventLoopGroup;
	}

	public NettyClientSocketOptions eventLoopGroup(NioEventLoopGroup eventLoopGroup) {
		this.eventLoopGroup = eventLoopGroup;
		return this;
	}

}
