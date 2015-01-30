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
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Abstract base class that implements common functionality shared by clients and servers.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class PeerStream<IN, OUT> extends Stream<ChannelStream<IN, OUT>> {

	private final Dispatcher dispatcher;

	private final Broadcaster<ChannelStream<IN, OUT>> open;
	private final Broadcaster<ChannelStream<IN, OUT>> close;
	private final Promise<PeerStream<IN, OUT>>        start;
	private final Promise<PeerStream<IN, OUT>>        shutdown;

	protected final long prefetch;

	private final FastList<OUT> writePublishers = new FastList<OUT>();
	private final Environment            env;
	private final Codec<Buffer, IN, OUT> defaultCodec;

	protected PeerStream(@Nonnull Environment env,
	                     @Nonnull Dispatcher dispatcher,
	                     @Nullable Codec<Buffer, IN, OUT> codec) {
		this(env, dispatcher, codec, Long.MAX_VALUE);
	}

	protected PeerStream(@Nonnull Environment env,
	                     @Nonnull Dispatcher dispatcher,
	                     @Nullable Codec<Buffer, IN, OUT> codec,
	                     long prefetch) {
		this.env = env;
		this.defaultCodec = codec;
		this.prefetch = prefetch > 0 ? prefetch : Long.MAX_VALUE;
		this.dispatcher = dispatcher;

		/*this.open = SynchronousDispatcher.INSTANCE == dispatcher ?
				Streams.<NetChannelStream<IN, OUT>>create(env) :
				Streams.<NetChannelStream<IN, OUT>>create(env, dispatcher);
		this.close = SynchronousDispatcher.INSTANCE == dispatcher ?
				Streams.<NetChannelStream<IN, OUT>>create(env) :
				Streams.<NetChannelStream<IN, OUT>>create(env, dispatcher);*/

		this.open = Broadcaster.<ChannelStream<IN, OUT>>create(env, dispatcher);
		this.close = Broadcaster.<ChannelStream<IN, OUT>>create(env, dispatcher);
		this.start = Promises.ready(env, dispatcher);
		this.shutdown = Promises.ready(env, dispatcher);
	}

	@Override
	public void subscribe(final Subscriber<? super ChannelStream<IN, OUT>> s) {
		start.onSuccess(new Consumer<PeerStream<IN, OUT>>() {
			@Override
			public void accept(PeerStream<IN, OUT> inoutNetPeerStream) {
				inoutNetPeerStream.open.subscribe(s);
			}
		});
	}

	/**
	 * Notify this server's consumers that the server has errors.
	 *
	 * @param t the error to signal
	 */
	final protected void notifyError(Throwable t) {
		open.onError(t);
	}

	/**
	 * Notify this server's consumers that the server has started.
	 */
	final protected void notifyStart() {
		start.onNext(this);
	}

	/**
	 * Notify this peer's consumers that the channel has been opened.
	 *
	 * @param channel The channel that was opened.
	 */
	final protected void notifyNewChannel(@Nonnull ChannelStream<IN, OUT> channel) {
		open.onNext(channel);
	}

	/**
	 * Notify this server's consumers that the server has stopped.
	 */
	final protected void notifyShutdown() {
		open.onComplete();
		close.onComplete();
		shutdown.onNext(this);
	}

	/**
	 * Implementing Write consumers for errors, batch requesting/flushing, write requests
	 * The routeChannel method will resolve the current channel consumers to execute and listen for returning signals.
	 */

	final protected void addWritePublisher(Publisher<? extends OUT> publisher) {
		synchronized (this) {
			writePublishers.add(publisher);
		}
	}

	/**
	 * Subclasses should implement this method and provide a {@link ChannelStream} object.
	 *
	 * @return The new {@link Channel} object.
	 */
	protected abstract ChannelStream<IN, OUT> createChannel(Object nativeChannel, long prefetch);

	protected Consumer<Throwable> createErrorConsumer(final ChannelStream<IN, OUT> ch) {
		return new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				try {
					ch.close();
					open.onError(throwable);
				} catch (Throwable t2) {
					open.onError(t2);
				}
			}
		};
	}

	protected Consumer<Void> createCompleteConsumer(final ChannelStream<IN, OUT> ch) {
		return new Consumer<Void>() {
			@Override
			public void accept(Void nothing) {
				try {
					ch.close();
				} catch (Throwable t2) {
					open.onError(t2);
				}
			}
		};
	}

	protected Function<? super OUT, Promise<Void>> createSendTask(final ChannelStream<IN, OUT> ch) {
		return new Function<OUT, Promise<Void>>() {
			@Override
			public Promise<Void> apply(OUT out) {
				return ch.echo(out);
			}
		};
	}

	protected Action<Long, Long> createBatchAction(
			final ChannelStream<IN, OUT> ch,
			final Consumer<Throwable> errorConsumer,
			final Consumer<Void> completeConsumer) {

		return new Action<Long, Long>() {
			boolean first = true;

			@Override
			protected void doNext(Long aLong) {
				if (first) {
					first = false;
				} else {
					ch.flush();
				}
				broadcastNext(aLong);
			}

			@Override
			protected void doComplete() {
				completeConsumer.accept(null);
				super.doComplete();
			}

			@Override
			protected void doError(Throwable ev) {
				errorConsumer.accept(ev);
				super.doError(ev);
			}
		};
	}

	protected Function<Stream<Long>, ? extends Publisher<? extends Long>> createAdaptiveDemandMapper(
			final ChannelStream<IN, OUT> ch,
			final Consumer<Throwable> errorConsumer,
			final Consumer<Void> completeConsumer
	) {
		return new Function<Stream<Long>, Publisher<? extends Long>>() {
			@Override
			public Publisher<? extends Long> apply(Stream<Long> requests) {
				return requests
						.broadcastTo(createBatchAction(ch, errorConsumer, completeConsumer));
			}
		};
	}

	protected Iterable<Publisher<? extends OUT>> routeChannel(ChannelStream<IN, OUT> ch) {
		return writePublishers;
	}

	@SuppressWarnings("unchecked")
	protected void mergeWrite(final ChannelStream<IN, OUT> ch) {
		Iterable<Publisher<? extends OUT>> publishers = routeChannel(ch);

		if (publishers == writePublishers) {
			int size;
			Publisher<OUT> publisher = null;
			synchronized (this) {
				size = writePublishers.size;
				if (size > 0) {
					publisher = writePublishers.array[0];
				}
			}

			if (size == 0) {
				if (ch.head() != null) {
					subscribeChannelHandlers(Streams.create(ch.head()), ch);
					return;
				}
				return;
			} else if (size == 1 && publisher != null) {
				subscribeChannelHandlers(Streams.create(publisher).startWith(ch.head()), ch);
				return;
			}
		}
		// TODO: what behavior do we want to implement?
		// Currently implemented: Streams.concat(publishers) execute handlers in order (handler chain)
		// Also possible: Streams.merge(publishers) all handlers contribute to the output stream
		// (be careful if on the same single threaded dispatcher it is still executed sequentially,
		// so maybe we need to specify another dispatcher if we decide to implement an "all handlers
		// contribute at the same time" behavior)
		subscribeChannelHandlers(
				Streams.concat(publishers).startWith(ch.head()),
				ch
		);
	}

	protected void subscribeChannelHandlers(final Stream<? extends OUT> writeStream, final ChannelStream<IN, OUT> ch) {
		final Consumer<Throwable> errorConsumer = createErrorConsumer(ch);
		final Consumer<Void> completeConsumer = createCompleteConsumer(ch);

		if (ch.getPrefetch() != Long.MAX_VALUE) {
			writeStream
					.dispatchOn(ch.getIODispatcher())
					.observe(ch.writeThrough())
					.capacity(ch.getPrefetch())
					.adaptiveConsume(null, createAdaptiveDemandMapper(ch, errorConsumer, completeConsumer));
		} else {
			writeStream
					.flatMap(createSendTask(ch))
					.consume(null, errorConsumer, completeConsumer);
		}
	}

	/**
	 * Get the {@link Codec} in use.
	 *
	 * @return The defaultCodec. May be {@literal null}.
	 */
	@Nullable
	public final Codec<Buffer, IN, OUT> getDefaultCodec() {
		return defaultCodec;
	}

	@Nonnull
	public final Environment getEnvironment() {
		return env;
	}

	public final long getPrefetchSize() {
		return prefetch;
	}

	@Nonnull
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}

	static final class FastList<T> implements Iterable<Publisher<? extends T>> {
		Publisher[] array;
		int         size;

		public void add(Publisher o) {
			int s = size;
			Publisher[] a = array;
			if (a == null) {
				a = new Publisher[16];
				array = a;
			} else if (s == a.length) {
				Publisher[] array2 = new Publisher[s + (s >> 2)];
				System.arraycopy(a, 0, array2, 0, s);
				a = array2;
				array = a;
			}
			a[s] = o;
			size = s + 1;
		}

		@Override
		public Iterator<Publisher<? extends T>> iterator() {
			return new Iterator<Publisher<? extends T>>() {
				int i = 0;

				@Override
				public boolean hasNext() {
					return i < size;
				}

				@Override
				@SuppressWarnings("unchecked")
				public Publisher<? extends T> next() {
					return (Publisher<? extends T>) array[i];
				}
			};
		}
	}
}
