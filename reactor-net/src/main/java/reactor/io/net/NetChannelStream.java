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
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.fn.batch.BatchConsumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * An abstract {@link NetChannel} implementation that handles the basic interaction and behave as a {@link
 * reactor.rx.Stream}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class NetChannelStream<IN, OUT> extends Stream<IN> implements NetChannel<IN, OUT> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected final Action<IN, IN> contentStream;

	private final Environment env;
	private final Dispatcher  ioDispatcher;
	private final Dispatcher  eventsDispatcher;

	private final Function<Buffer, IN>  decoder;
	private final Function<OUT, Buffer> encoder;

	protected NetChannelStream(@Nonnull Environment env,
	                           @Nullable Codec<Buffer, IN, OUT> codec,
	                           @Nonnull Dispatcher ioDispatcher,
	                           @Nonnull Dispatcher eventsDispatcher) {
		Assert.notNull(env, "IO Dispatcher cannot be null");
		Assert.notNull(env, "Events Reactor cannot be null");
		this.env = env;
		this.ioDispatcher = ioDispatcher;
		this.eventsDispatcher = eventsDispatcher;
		this.contentStream =  new Action<IN, IN>(eventsDispatcher) {
			@Override
			protected void doNext(IN ev) {
				broadcastNext(ev);
			}
		}.env(getEnvironment());

		if (null != codec) {
			this.decoder = codec.decoder(contentStream);
			this.encoder = codec.encoder();
		} else {
			this.decoder = null;
			this.encoder = null;
		}
	}

	@Override
	public Stream<IN> in() {
		return contentStream;
	}

	@Override
	public BatchConsumer<OUT> out() {
		return new WriteConsumer(null);
	}

	@Override
	public void subscribe(Subscriber<? super IN> s) {
		contentStream.subscribe(s);
	}

	@Override
	public Promise<Void> send(OUT data) {
		Promise<Void> d = Promises.ready(env, eventsDispatcher);
		send(data, d);
		return d;
	}

	@Override
	public NetChannelStream<IN, OUT> echo(OUT data) {
		send(data, null);
		return this;
	}

	@Override
	public NetChannelStream<IN, OUT> echoFrom(Publisher<? extends OUT> dataStream) {
		dataStream.subscribe(new Subscriber<OUT>() {
			@Override
			public void onComplete() {
				//IGNORE
			}

			@Override
			public void onError(Throwable t) {
				contentStream.onError(t);
			}

			@Override
			public void onNext(OUT out) {
				send(out, null);
			}

			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}
		});
		return this;
	}

	public Promise<IN> sendAndReceive(OUT request){
		Promise<IN> promise = contentStream.next();
		echo(request);
		return promise;
	}

	/**
	 * Send data on this connection. The current codec (if any) will be used to encode the data to a {@link
	 * reactor.io.buffer.Buffer}. The given callback will be invoked when the write has completed.
	 *
	 * @param data       The outgoing data.
	 * @param onComplete The callback to invoke when the write is complete.
	 */
	protected void send(OUT data, final Promise<Void> onComplete) {
		ioDispatcher.dispatch(data, new WriteConsumer(onComplete), null);
	}

	@Override
	public void close() {
		contentStream.onComplete();
	}

	/**
	 * Performing necessary decoding on the data and notify the internal {@link Broadcaster} of any results.
	 *
	 * @param data The data to decode.
	 * @return {@literal true} if any more data is remaining to be consumed in the given {@link Buffer}, {@literal false}
	 * otherwise.
	 */
	@SuppressWarnings("unchecked")
	public boolean read(Buffer data) {
		if (null != decoder && null != data.byteBuffer()) {
			decoder.apply(data);
		} else {
			contentStream.onNext((IN)data);
		}

		return data.remaining() > 0;
	}

	public void notifyRead(IN obj) {
		contentStream.onNext(obj);
	}

	public void notifyError(Throwable throwable) {
		contentStream.onError(throwable);
	}

	public void notifyClose() {
		contentStream.onComplete();
	}

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write, as a {@link Buffer}.
	 * @param onComplete The callback to invoke when the write is complete.
	 */
	protected void write(Buffer data, Promise<Void> onComplete, boolean flush) {
		write(data.byteBuffer(), onComplete, flush);
	}

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write.
	 * @param onComplete The callback to invoke when the write is complete.
	 * @param flush      whether to flush the underlying IO channel
	 */
	protected abstract void write(ByteBuffer data, Promise<Void> onComplete, boolean flush);

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write.
	 * @param onComplete The callback to invoke when the write is complete.
	 * @param flush      whether to flush the underlying IO channel
	 */
	protected abstract void write(Object data, Promise<Void> onComplete, boolean flush);

	/**
	 * Subclasses must implement this method to perform IO flushes.
	 */
	protected abstract void flush();

	@Override
	public Environment getEnvironment() {
		return env;
	}

	@Override
	public Dispatcher getDispatcher() {
		return eventsDispatcher;
	}

	public Function<Buffer, IN> getDecoder() {
		return decoder;
	}

	public Function<OUT, Buffer> getEncoder() {
		return encoder;
	}

	private final class WriteConsumer implements BatchConsumer<OUT> {
		private final Promise<Void> onComplete;
		private volatile boolean autoflush = true;

		private WriteConsumer(Promise<Void> onComplete) {
			this.onComplete = onComplete;
		}

		@Override
		public void start() {
			autoflush = false;
		}

		@Override
		public void end() {
			flush();
			autoflush = true;
		}

		@Override
		public void accept(OUT data) {
			try {
				if (null != encoder) {
					Buffer bytes = encoder.apply(data);
					if (bytes.remaining() > 0) {
						write(bytes, onComplete, autoflush);
					}
				} else {
					if (Buffer.class.isInstance(data)) {
						write((Buffer) data, onComplete, autoflush);
					} else {
						write(data, onComplete, autoflush);
					}
				}
			} catch (Throwable t) {
				contentStream.onError(t);
				if (null != onComplete) {
					onComplete.onError(t);
				}
			}
		}
	}

}
