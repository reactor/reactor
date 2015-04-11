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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.IOStreams;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.Control;
import reactor.rx.broadcast.Broadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * An abstract {@link Channel} implementation that handles the basic interaction and behave as a {@link
 * reactor.rx.Stream}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class ChannelStream<IN, OUT> extends Stream<IN> implements Channel<IN, OUT> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected final PeerStream<IN, OUT, ChannelStream<IN, OUT>> peer;
	protected final Broadcaster<IN>     contentStream;
	private final   Environment         env;

	private final Dispatcher ioDispatcher;
	private final Dispatcher eventsDispatcher;

	private final Function<Buffer, IN>  decoder;
	private final Function<OUT, Buffer> encoder;
	private final long                  prefetch;

	protected ChannelStream(final @Nonnull Environment env,
	                        @Nullable Codec<Buffer, IN, OUT> codec,
	                        long prefetch,
	                        @Nonnull PeerStream<IN, OUT, ChannelStream<IN, OUT>> peer,
	                        @Nonnull Dispatcher ioDispatcher,
	                        @Nonnull Dispatcher eventsDispatcher) {
		Assert.notNull(env, "IO Dispatcher cannot be null");
		Assert.notNull(env, "Events Reactor cannot be null");
		this.env = env;
		this.prefetch = prefetch;
		this.ioDispatcher = ioDispatcher;
		this.peer = peer;
		this.eventsDispatcher = eventsDispatcher;
		this.contentStream = Broadcaster.<IN>create(env, eventsDispatcher);

		if (null != codec) {
			this.decoder = codec.decoder(new Consumer<IN>() {
				@Override
				public void accept(IN in) {
					doDecoded(in);
				}
			});
			this.encoder = codec.encoder();
		} else {
			this.decoder = null;
			this.encoder = null;
		}
	}

	@Override
	public void subscribe(Subscriber<? super IN> s) {
		contentStream.subscribe(s);
	}

	@Override
	public Control sink(Publisher<? extends OUT> source) {
		return peer.subscribeChannelHandlers(Streams.create(source), this);
	}

	final public Control sinkBuffers(Publisher<? extends Buffer> source) {
		Stream<OUT> encodedSource = Streams.create(source).map(new Function<Buffer, OUT>() {
			@Override
			@SuppressWarnings("unchecked")
			public OUT apply(Buffer data) {
				if (null != encoder) {
					Buffer bytes = encoder.apply((OUT) data);
					return (OUT) bytes;
				} else {
					return (OUT) data;
				}
			}
		});

		return sink(encodedSource);
	}

	@Override
	public final Environment getEnvironment() {
		return env;
	}

	@Override
	public final Dispatcher getDispatcher() {
		return eventsDispatcher;
	}

	@Override
	final public long getCapacity() {
		return prefetch;
	}

	public final Dispatcher getIODispatcher() {
		return ioDispatcher;
	}

	public final Function<Buffer, IN> getDecoder() {
		return decoder;
	}

	public final Function<OUT, Buffer> getEncoder() {
		return encoder;
	}

	/**
	 * Direct access to receiving side - should be used to forward incoming data manually or testing purpose
	 *
	 * @return a writer for the current ChannelStream consumers
	 */
	final public Subscriber<IN> in() {
		return contentStream;
	}

	/**
	 * Convert the current stream data into the decoded type produced by the passed codec
	 *
	 * @return the decoded stream
	 */
	final public <DECODED> Stream<DECODED> decode(Codec<IN, DECODED, ?> codec){
		return IOStreams.decode(codec, this);
	}

	/**
	 * @return the underlying native connection/channel in use
	 */
	public abstract Object delegate();

	/**
	 * notify Peer subscribers the channel has been created and attach the Peer defined writer Publishers
	 */
	public void registerOnPeer() {
		peer.notifyNewChannel(this);
		peer.mergeWrite(this);
	}

	Consumer<OUT> writeThrough(boolean autoflush) {
		return new WriteConsumer(autoflush);
	}

	protected void doDecoded(IN in) {
		contentStream.onNext(in);
	}

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write, as a {@link Buffer}.
	 * @param onComplete The callback to invoke when the write is complete.
	 */
	protected void write(Buffer data, Subscriber<?> onComplete, boolean flush) {
		if(data.byteBuffer() != null) {
			write(data.byteBuffer(), onComplete, flush);
		}
	}

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write.
	 * @param onComplete The callback to invoke when the write is complete.
	 * @param flush      whether to flush the underlying IO channel
	 */
	protected abstract void write(ByteBuffer data, Subscriber<?> onComplete, boolean flush);

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write.
	 * @param onComplete The callback to invoke when the write is complete.
	 * @param flush      whether to flush the underlying IO channel
	 */
	protected abstract void write(Object data, Subscriber<?> onComplete, boolean flush);

	/**
	 * Subclasses must implement this method to perform IO flushes.
	 */
	protected abstract void flush();

	final class WriteConsumer implements Consumer<OUT> {

		final boolean autoflush;

		public WriteConsumer(boolean autoflush) {
			this.autoflush = autoflush;
		}

		@Override
		public void accept(OUT data) {
		try {
				if (null != encoder) {
					Buffer bytes = encoder.apply(data);
					if (bytes.remaining() > 0) {
						write(bytes, null, autoflush);
					}
				} else {
					if (Buffer.class == data.getClass()) {
						write((Buffer) data, null, autoflush);
					} else {
						write(data, null, autoflush);
					}
				}
			} catch (Throwable t) {
				peer.notifyError(t);
			}
		}
	}

}
