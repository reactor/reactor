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
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

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
	private final Promise<NetPeerStream<IN, OUT>>        start;
	private final Promise<NetPeerStream<IN, OUT>>        shutdown;

	private final Environment            env;
	private final Codec<Buffer, IN, OUT> codec;

	protected NetPeerStream(@Nonnull Environment env,
	                        @Nonnull Dispatcher dispatcher,
	                        @Nullable Codec<Buffer, IN, OUT> codec) {
		this.env = env;
		this.codec = codec;
		this.dispatcher = dispatcher;

		/*this.open = SynchronousDispatcher.INSTANCE == dispatcher ?
				Streams.<NetChannelStream<IN, OUT>>create(env) :
				Streams.<NetChannelStream<IN, OUT>>create(env, dispatcher);
		this.close = SynchronousDispatcher.INSTANCE == dispatcher ?
				Streams.<NetChannelStream<IN, OUT>>create(env) :
				Streams.<NetChannelStream<IN, OUT>>create(env, dispatcher);*/

		this.open = Broadcaster.<NetChannelStream<IN, OUT>>create(env, dispatcher);
		this.close = Broadcaster.<NetChannelStream<IN, OUT>>create(env, dispatcher);
		this.start = Promises.ready(env, dispatcher);
		this.shutdown = Promises.ready(env, dispatcher);
	}

	@Override
	public void subscribe(final Subscriber<? super NetChannelStream<IN, OUT>> s) {
		start.onSuccess(new Consumer<NetPeerStream<IN, OUT>>() {
			@Override
			public void accept(NetPeerStream<IN, OUT> inoutNetPeerStream) {
				inoutNetPeerStream.open.subscribe(s);
			}
		});
		if(!start.isComplete()) {
			try {
				start.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				s.onError(e);
			}
		}
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
		open.onComplete();
		close.onComplete();
		shutdown.onNext(this);
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
	public final Environment getEnvironment() {
		return env;
	}

	@Nonnull
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}
}
