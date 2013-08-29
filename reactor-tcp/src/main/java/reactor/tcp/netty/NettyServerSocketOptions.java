package reactor.tcp.netty;

import io.netty.channel.ChannelPipeline;
import reactor.function.Consumer;
import reactor.tcp.config.ServerSocketOptions;

/**
 * Extends standard {@link ServerSocketOptions} with Netty-specific options.
 *
 * @author Jon Brisbin
 */
public class NettyServerSocketOptions extends ServerSocketOptions {

	private Consumer<ChannelPipeline> pipelineConfigurer;

	public Consumer<ChannelPipeline> pipelineConfigurer() {
		return pipelineConfigurer;
	}

	public NettyServerSocketOptions pipelineConfigurer(Consumer<ChannelPipeline> pipelineConfigurer) {
		this.pipelineConfigurer = pipelineConfigurer;
		return this;
	}

}
