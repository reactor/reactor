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

package reactor.io.net;

import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.Broadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract base class that implements common functionality shared by clients and servers.
 *
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class NetPeerStream<IN, OUT> extends Stream<NetChannelStream<IN, OUT>> {

	private final Dispatcher dispatcher;

	private final Broadcaster<NetChannelStream<IN, OUT>> open;
	private final Broadcaster<NetChannelStream<IN, OUT>> close;
	private final Broadcaster<NetPeerStream<IN, OUT>>    start;
	private final Broadcaster<NetPeerStream<IN, OUT>>    shutdown;

	private final Environment            env;
	private final Codec<Buffer, IN, OUT> codec;

	protected NetPeerStream(@Nonnull Environment env,
	                        @Nonnull Dispatcher dispatcher,
	                        @Nullable Codec<Buffer, IN, OUT> codec) {
		this.env = env;
		this.codec = codec;
		this.dispatcher = dispatcher;

		this.open = Streams.broadcast(env, dispatcher);
		this.close = Streams.broadcast(env, dispatcher);
		this.start = Streams.broadcast(env, dispatcher);
		this.shutdown = Streams.broadcast(env, dispatcher);
	}

	@Override
	public void subscribe(Subscriber<? super NetChannelStream<IN, OUT>> s) {
		open.subscribe(s);
		notifyStart();
	}

	/**
	 * Subclasses should implement this method and provide a {@link NetChannelStream} object.
	 *
	 * @return The new {@link NetChannel} object.
	 */
	protected abstract NetChannelStream<IN, OUT> createChannel(Object nativeChannel);

	/**
	 * Notify this server's consumers that the server has started.
	 */
	protected void notifyStart() {
		start.onNext(this);
		start.onComplete();
	}

	/**
	 * Notify this peer's consumers that the channel has been opened.
	 *
	 * @param channel The channel that was opened.
	 */
	protected void notifyNewChannel(@Nonnull NetChannelStream<IN, OUT> channel) {
		open.onNext(channel);
	}

	/**
	 * Notify this server's consumers that the server has stopped.
	 */
	protected void notifyShutdown() {
		shutdown.onNext(this);
		start.onComplete();
		open.onComplete();
		close.onComplete();
		shutdown.onComplete();
	}

	/**
	 * Get the {@link Codec} in use.
	 *
	 * @return The codec. May be {@literal null}.
	 */
	@Nullable
	protected Codec<Buffer, IN, OUT> getCodec() {
		return codec;
	}

	@Nonnull
	public Environment getEnvironment() {
		return env;
	}

	@Nonnull
	public Dispatcher getDispatcher() {
		return dispatcher;
	}
}
