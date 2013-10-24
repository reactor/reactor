/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.tcp;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.core.composable.spec.Streams;
import reactor.core.spec.Reactors;
import reactor.core.support.NotifyConsumer;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.event.support.EventConsumer;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.batch.BatchConsumer;
import reactor.io.Buffer;
import reactor.queue.BlockingQueueFactory;
import reactor.tcp.encoding.Codec;
import reactor.tuple.Tuple2;

import java.util.NoSuchElementException;
import java.util.Queue;

import static reactor.event.selector.Selectors.$;

/**
 * Implementations of this class should provide concrete functionality for doing real IO.
 *
 * @param <IN>
 * 		The type that will be received by this connection
 * @param <OUT>
 * 		The type that will be sent by this connection
 *
 * @author Jon Brisbin
 */
public abstract class AbstractTcpConnection<IN, OUT> implements TcpConnection<IN, OUT> {

	protected final long                     created = System.currentTimeMillis();
	protected final Tuple2<Selector, Object> read    = $();

	protected final Environment           env;
	protected final Dispatcher            ioDispatcher;
	protected final Reactor               ioReactor;
	protected final Reactor               eventsReactor;
	protected final Function<Buffer, IN>  decoder;
	protected final Function<OUT, Buffer> encoder;
	protected final Queue<Object>         replyToKeys;

	protected AbstractTcpConnection(Environment env,
	                                Codec<Buffer, IN, OUT> codec,
	                                Dispatcher ioDispatcher,
	                                Reactor eventsReactor) {
		this.env = env;
		this.ioDispatcher = ioDispatcher;
		this.ioReactor = Reactors.reactor(env, ioDispatcher);
		this.eventsReactor = eventsReactor;
		if(null != codec) {
			this.decoder = codec.decoder(new NotifyConsumer<IN>(read.getT2(), eventsReactor));
			this.encoder = codec.encoder();
		} else {
			this.decoder = null;
			this.encoder = null;
		}
		this.replyToKeys = BlockingQueueFactory.createQueue();

		consume(new Consumer<IN>() {
			@Override
			public void accept(IN in) {
				try {
					AbstractTcpConnection.this.eventsReactor.notify(replyToKeys.remove(), Event.wrap(in));
				} catch(NoSuchElementException ignored) {
				}
			}
		});
	}

	/**
	 * Get the {@code System.currentTimeMillis()} this connection was created.
	 *
	 * @return creation time
	 */
	public long getCreated() {
		return created;
	}

	@Override
	public void close() {
		eventsReactor.getConsumerRegistry().unregister(read.getT2());
	}

	@Override
	public Stream<IN> in() {
		final Deferred<IN, Stream<IN>> d = Streams.<IN>defer(env, eventsReactor.getDispatcher());
		consume(new Consumer<IN>() {
			@Override
			public void accept(IN in) {
				d.accept(in);
			}
		});
		return d.compose();
	}

	@Override
	public BatchConsumer<OUT> out() {
		return new WriteConsumer(null);
	}

	@Override
	public <T extends Throwable> TcpConnection<IN, OUT> when(Class<T> errorType, Consumer<T> errorConsumer) {
		eventsReactor.on(Selectors.T(errorType), new EventConsumer<T>(errorConsumer));
		return this;
	}

	@Override
	public TcpConnection<IN, OUT> consume(final Consumer<IN> consumer) {
		eventsReactor.on(read.getT1(), new Consumer<Event<IN>>() {
			@Override
			public void accept(Event<IN> ev) {
				consumer.accept(ev.getData());
			}
		});
		return this;
	}

	@Override
	public TcpConnection<IN, OUT> receive(final Function<IN, OUT> fn) {
		consume(new Consumer<IN>() {
			@Override
			public void accept(IN in) {
				send(fn.apply(in));
			}
		});
		return this;
	}

	@Override
	public TcpConnection<IN, OUT> send(Stream<OUT> data) {
		data.consume(new Consumer<OUT>() {
			@Override
			public void accept(OUT out) {
				send(out, null);
			}
		});
		return this;
	}

	@Override
	public Promise<Void> send(OUT data) {
		Deferred<Void, Promise<Void>> d = Promises.defer(env, eventsReactor.getDispatcher());
		send(data, d);
		return d.compose();
	}

	@Override
	public TcpConnection<IN, OUT> sendAndForget(OUT data) {
		send(data, null);
		return this;
	}

	@Override
	public Promise<IN> sendAndReceive(OUT data) {
		final Deferred<IN, Promise<IN>> d = Promises.defer(env, eventsReactor.getDispatcher());
		Tuple2<Selector, Object> tup = $();
		eventsReactor.on(tup.getT1(), new EventConsumer<IN>(d)).cancelAfterUse();
		replyToKeys.add(tup.getT2());
		send(data, null);
		return d.compose();
	}

	/**
	 * Send data on this connection. The current codec (if any) will be used to encode the data to a {@link
	 * reactor.io.Buffer}. The given callback will be invoked when the write has completed.
	 *
	 * @param data
	 * 		The outgoing data.
	 * @param onComplete
	 * 		The callback to invoke when the write is complete.
	 */
	protected void send(OUT data, final Deferred<Void, Promise<Void>> onComplete) {
		Reactors.schedule(new WriteConsumer(onComplete), data, ioReactor);
	}

	/**
	 * Perfoming necessary decoding on the data and notify the internal {@link Reactor} of any results.
	 *
	 * @param data
	 * 		The data to decode.
	 *
	 * @return {@literal true} if any more data is remaining to be consumed in the given {@link Buffer}, {@literal false}
	 * otherwise.
	 */
	public boolean read(Buffer data) {
		if(null != decoder && null != data.byteBuffer()) {
			decoder.apply(data);
		} else {
			eventsReactor.notify(read.getT2(), Event.wrap(data));
		}

		return data.remaining() > 0;
	}

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data
	 * 		The data to write, as a {@link Buffer}.
	 * @param onComplete
	 * 		The callback to invoke when the write is complete.
	 */
	protected abstract void write(Buffer data, Deferred<Void, Promise<Void>> onComplete, boolean flush);

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data
	 * 		The data to write.
	 * @param onComplete
	 * 		The callback to invoke when the write is complete.
	 */
	protected abstract void write(Object data, Deferred<Void, Promise<Void>> onComplete, boolean flush);

	/**
	 * Subclasses must implement this method to perform IO flushes.
	 */
	protected abstract void flush();

	private final class WriteConsumer implements BatchConsumer<OUT> {
		private final Deferred<Void, Promise<Void>> onComplete;
		private volatile boolean autoflush = true;

		private WriteConsumer(Deferred<Void, Promise<Void>> onComplete) {
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
				if(null != encoder) {
					Buffer bytes = encoder.apply(data);
					if(bytes.remaining() > 0) {
						write(bytes, onComplete, autoflush);
					}
				} else {
					if(Buffer.class.isInstance(data)) {
						write((Buffer)data, onComplete, autoflush);
					} else {
						write(data, onComplete, autoflush);
					}
				}
			} catch(Throwable t) {
				eventsReactor.notify(t.getClass(), Event.wrap(t));
				if(null != onComplete) {
					onComplete.accept(t);
				}
			}
		}
	}

}
