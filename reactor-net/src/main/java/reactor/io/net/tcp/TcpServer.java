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

package reactor.io.net.tcp;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.PeerStream;
import reactor.io.net.Server;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.rx.Promise;
import reactor.rx.broadcast.Broadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;

/**
 * Base functionality needed by all servers that communicate with clients over TCP.
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class TcpServer<IN, OUT>
		extends PeerStream<IN, OUT>
		implements Server<IN, OUT, ChannelStream<IN, OUT>> {

	private final InetSocketAddress   listenAddress;
	private final ServerSocketOptions options;
	private final SslOptions          sslOptions;

	protected TcpServer(@Nonnull Environment env,
	                    @Nonnull Dispatcher dispatcher,
	                    @Nullable InetSocketAddress listenAddress,
	                    ServerSocketOptions options,
	                    SslOptions sslOptions,
	                    @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, dispatcher, codec);
		this.listenAddress = listenAddress;
		Assert.notNull(options, "ServerSocketOptions cannot be null");
		this.options = options;
		this.sslOptions = sslOptions;
	}

	@Override
	public Server<IN, OUT, ChannelStream<IN, OUT>> service(
			final BiConsumer<ChannelStream<IN, OUT>, Subscriber<? super OUT>> serviceConsumer) {
		consume(new Consumer<ChannelStream<IN, OUT>>() {
			@Override
			public void accept(ChannelStream<IN, OUT> inoutChannelStream) {
				Broadcaster<OUT> b = Broadcaster.create(getEnvironment(), getDispatcher());
				addWritePublisher(b);
				serviceConsumer.accept(inoutChannelStream, b);
			}
		}, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				notifyError(throwable);
			}
		});
		return this;
	}

	@Override
	public Server<IN, OUT, ChannelStream<IN, OUT>> service(
			Function<ChannelStream<IN, OUT>, ? extends Publisher<? extends OUT>> serviceFunction) {
		consume(new Consumer<ChannelStream<IN, OUT>>() {
			@Override
			public void accept(ChannelStream<IN, OUT> inoutChannelStream) {
				addWritePublisher(serviceFunction.apply(inoutChannelStream));
			}
		}, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				notifyError(throwable);
			}
		});
		return this;
	}

	/**
	 * Start this server.
	 *
	 * @return {@literal this}
	 */
	public abstract Promise<Void> start();

	/**
	 * Get the address to which this server is bound.
	 *
	 * @return
	 */
	protected InetSocketAddress getListenAddress() {
		return listenAddress;
	}

	/**
	 * Get the {@link reactor.io.net.config.ServerSocketOptions} currently in effect.
	 *
	 * @return
	 */
	protected ServerSocketOptions getOptions() {
		return options;
	}

	/**
	 * Get the {@link reactor.io.net.config.SslOptions} current in effect.
	 *
	 * @return the SSL options
	 */
	protected SslOptions getSslOptions() {
		return sslOptions;
	}

}
