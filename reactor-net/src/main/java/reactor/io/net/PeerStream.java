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
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.Action;
import reactor.rx.action.Control;
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
public abstract class PeerStream<IN, OUT, CONN extends ChannelStream<IN, OUT>> extends Stream<CONN> {

	private final Dispatcher dispatcher;

	protected final Broadcaster<CONN> channels;
	protected final long              prefetch;

	private final FastList<OUT> writePublishers = new FastList<OUT>();
	private final Environment            env;
	private final Codec<Buffer, IN, OUT> defaultCodec;

	protected PeerStream(Environment env,
	                     Dispatcher dispatcher,
	                     Codec<Buffer, IN, OUT> codec) {
		this(env, dispatcher, codec, Long.MAX_VALUE);
	}

	protected PeerStream(Environment env,
	                     Dispatcher dispatcher,
	                     Codec<Buffer, IN, OUT> codec,
	                     long prefetch) {
		this.env = env == null && Environment.alive() ? Environment.get() : env;
		this.defaultCodec = codec;
		this.prefetch = prefetch > 0 ? prefetch : Long.MAX_VALUE;
		this.dispatcher = dispatcher != null ? dispatcher : SynchronousDispatcher.INSTANCE;

		/*this.channels = SynchronousDispatcher.INSTANCE == dispatcher ?
				Streams.<NetChannelStream<IN, OUT>>create(env) :
				Streams.<NetChannelStream<IN, OUT>>create(env, dispatcher);
		this.close = SynchronousDispatcher.INSTANCE == dispatcher ?
				Streams.<NetChannelStream<IN, OUT>>create(env) :
				Streams.<NetChannelStream<IN, OUT>>create(env, dispatcher);*/

		this.channels = Broadcaster.<CONN>create(env, SynchronousDispatcher.INSTANCE);
	}

	@Override
	public void subscribe(final Subscriber<? super CONN> s) {
		channels.subscribe(s);
	}

	protected void doPipeline(
			final Function<? super CONN, ? extends Publisher<? extends OUT>> serviceFunction) {
		consume(new Consumer<CONN>() {
			@Override
			public void accept(CONN inoutChannelStream) {
				addWritePublisher(serviceFunction.apply(inoutChannelStream));
			}
		}, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				notifyError(throwable);
			}
		});
	}


	@Override
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}

	/**
	 * Notify this server's consumers that the server has errors.
	 *
	 * @param t the error to signal
	 */
	final protected void notifyError(Throwable t) {
		channels.onError(t);
	}

	/**
	 * Notify this peer's consumers that the channel has been opened.
	 *
	 * @param channel The channel that was opened.
	 */
	final protected void notifyNewChannel(CONN channel) {
		channels.onNext(channel);
	}

	/**
	 * Notify this server's consumers that the server has stopped.
	 */
	final protected void notifyShutdown() {
		channels.onComplete();
	}

	/**
	 * Implementing Write consumers for errors, batch requesting/flushing, write requests
	 * The routeChannel method will resolve the current channel consumers to execute and listen for returning signals.
	 */

	final protected Publisher<? extends OUT> addWritePublisher(Publisher<? extends OUT> publisher) {
		synchronized (this) {
			writePublishers.add(publisher);
			return publisher;
		}
	}

	/**
	 * Subclasses should implement this method and provide a {@link ChannelStream} object.
	 *
	 * @return the bound Reactor Channel Stream
	 */
	protected abstract CONN bindChannel(Object nativeChannel, long prefetch);

	protected Consumer<Throwable> createErrorConsumer(final ChannelStream<IN, OUT> ch) {
		return new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				try {
					channels.onError(throwable);
				} catch (Throwable t2) {
					channels.onError(t2);
				}
			}
		};
	}

	protected Action<Long, Long> createBatchAction(
			final CONN ch,
			final Consumer<Throwable> errorConsumer,
			final Consumer<Void> completionConsumer
	) {

		return new Action<Long, Long>() {
			boolean first = true;

			@Override
			protected void doNext(Long aLong) {
				shouldFlush();
				broadcastNext(aLong);
			}

			@Override
			protected void doComplete() {
				ch.flush();
				super.doComplete();
			}

			@Override
			protected void doError(Throwable ev) {
				errorConsumer.accept(ev);
				super.doError(ev);
			}

			private void shouldFlush() {
				if (first) {
					first = false;
				} else {
					ch.flush();
				}
			}
		};
	}

	protected Function<Stream<Long>, ? extends Publisher<? extends Long>> createAdaptiveDemandMapper(
			final CONN ch,
			final Consumer<Throwable> errorConsumer
	) {
		return new Function<Stream<Long>, Publisher<? extends Long>>() {
			@Override
			public Publisher<? extends Long> apply(Stream<Long> requests) {
				return requests
						.broadcastTo(createBatchAction(ch, errorConsumer, completeConsumer(ch)));
			}
		};
	}

	protected Iterable<Publisher<? extends OUT>> routeChannel(CONN ch) {
		return writePublishers;
	}

	@SuppressWarnings("unchecked")
	protected Control mergeWrite(final CONN ch) {
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
				return null;
			} else if (size == 1 && publisher != null) {
				return subscribeChannelHandlers(Streams.create(publisher), ch);
			}
		}
		// TODO: what behavior do we want to implement?
		// Currently implemented: Streams.concat(publishers) execute handlers in order (handler chain)
		// Also possible: Streams.merge(publishers) all handlers contribute to the output stream
		// (be careful if on the same single threaded dispatcher it is still executed sequentially,
		// so maybe we need to specify another dispatcher if we decide to implement an "all handlers
		// contribute at the same time" behavior)
		return subscribeChannelHandlers(
				Streams.concat(publishers),
				ch
		);
	}

	protected Consumer<Void> completeConsumer(CONN ch) {
		return null;
	}

	protected Control subscribeChannelHandlers(Stream<? extends OUT> writeStream, final CONN ch) {
		if (writeStream.getCapacity() != Long.MAX_VALUE) {
			return writeStream
					.dispatchOn(ch.getIODispatcher())
					.adaptiveConsume(ch.writeThrough(false),
							createAdaptiveDemandMapper(ch,
									createErrorConsumer(ch)));
		} else {
			return writeStream
					.dispatchOn(ch.getIODispatcher())
					.consume(ch.writeThrough(true), createErrorConsumer(ch), completeConsumer(ch));
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
				public void remove() {
					throw new UnsupportedOperationException("");
				}

				@Override
				@SuppressWarnings("unchecked")
				public Publisher<? extends T> next() {
					return (Publisher<? extends T>) array[i++];
				}
			};
		}
	}
}
