package reactor.net.netty;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import reactor.function.Consumer;
import reactor.net.config.ClientSocketOptions;

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
