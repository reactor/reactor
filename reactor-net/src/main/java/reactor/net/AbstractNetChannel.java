package reactor.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.rx.Deferred;
import reactor.rx.Promise;
import reactor.rx.Stream;
import reactor.rx.spec.Promises;
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
import reactor.io.encoding.Codec;
import reactor.queue.BlockingQueueFactory;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Queue;

import static reactor.event.selector.Selectors.$;

/**
 * An abstract {@link reactor.net.NetChannel} implementation that handles the basic interaction and {@link
 * reactor.rx.Stream} and {@link reactor.function.Consumer} handling.
 *
 * @author Jon Brisbin
 */
public abstract class AbstractNetChannel<IN, OUT> implements NetChannel<IN, OUT> {

	protected final Logger   log  = LoggerFactory.getLogger(getClass());
	private final   Selector read = $();

	private final Environment            env;
	private final Reactor                ioReactor;
	private final Reactor                eventsReactor;
	private final Codec<Buffer, IN, OUT> codec;
	private final Function<Buffer, IN>   decoder;
	private final Function<OUT, Buffer>  encoder;
	private final Queue<Object>          replyToKeys;

	protected AbstractNetChannel(@Nonnull Environment env,
	                             @Nullable Codec<Buffer, IN, OUT> codec,
	                             @Nonnull Dispatcher ioDispatcher,
	                             @Nonnull Reactor eventsReactor) {
		Assert.notNull(env, "IO Dispatcher cannot be null");
		Assert.notNull(env, "Events Reactor cannot be null");
		this.env = env;
		this.ioReactor = Reactors.reactor(env, ioDispatcher);
		this.eventsReactor = Reactors.reactor(env, eventsReactor.getDispatcher());
		this.codec = codec;
		if (null != codec) {
			this.decoder = codec.decoder(new NotifyConsumer<IN>(read.getObject(), this.eventsReactor));
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
					if (!replyToKeys.isEmpty()) {
						AbstractNetChannel.this.eventsReactor.notify(replyToKeys.remove(), Event.wrap(in));
					}
				} catch (NoSuchElementException ignored) {
				}
			}
		});
	}

	public Function<Buffer, IN> getDecoder() {
		return decoder;
	}

	public Function<OUT, Buffer> getEncoder() {
		return encoder;
	}

	@Override
	public Stream<IN> in() {
		final Deferred<IN, Stream<IN>> d = new Deferred<IN, Stream<IN>>(new Stream<IN>(eventsReactor, -1, null, env));
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
	public <T extends Throwable> NetChannel<IN, OUT> when(Class<T> errorType, Consumer<T> errorConsumer) {
		eventsReactor.on(Selectors.T(errorType), new EventConsumer<T>(errorConsumer));
		return this;
	}

	@Override
	public NetChannel<IN, OUT> consume(final Consumer<IN> consumer) {
		eventsReactor.on(read, new Consumer<Event<IN>>() {
			@Override
			public void accept(Event<IN> ev) {
				consumer.accept(ev.getData());
			}
		});
		return this;
	}

	@Override
	public NetChannel<IN, OUT> receive(final Function<IN, OUT> fn) {
		consume(new Consumer<IN>() {
			@Override
			public void accept(IN in) {
				send(fn.apply(in));
			}
		});
		return this;
	}

	@Override
	public NetChannel<IN, OUT> send(Stream<OUT> data) {
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
	public NetChannel<IN, OUT> sendAndForget(OUT data) {
		send(data, null);
		return this;
	}

	@Override
	public Promise<IN> sendAndReceive(OUT data) {
		final Deferred<IN, Promise<IN>> d = Promises.defer(env, eventsReactor.getDispatcher());
		Selector sel = $();
		eventsReactor.on(sel, new EventConsumer<IN>(d)).cancelAfterUse();
		replyToKeys.add(sel.getObject());
		send(data, null);
		return d.compose();
	}

	@Override
	public Promise<Boolean> close() {
		Deferred<Boolean, Promise<Boolean>> d = Promises.defer(getEnvironment(), eventsReactor.getDispatcher());
		eventsReactor.getConsumerRegistry().unregister(read.getObject());
		close(d);
		return d.compose();
	}

	/**
	 * Send data on this connection. The current codec (if any) will be used to encode the data to a {@link
	 * reactor.io.Buffer}. The given callback will be invoked when the write has completed.
	 *
	 * @param data       The outgoing data.
	 * @param onComplete The callback to invoke when the write is complete.
	 */
	protected void send(OUT data, final Deferred<Void, Promise<Void>> onComplete) {
		ioReactor.schedule(new WriteConsumer(onComplete), data);
	}

	/**
	 * Performing necessary decoding on the data and notify the internal {@link Reactor} of any results.
	 *
	 * @param data The data to decode.
	 * @return {@literal true} if any more data is remaining to be consumed in the given {@link Buffer}, {@literal false}
	 * otherwise.
	 */
	public boolean read(Buffer data) {
		if (null != decoder && null != data.byteBuffer()) {
			decoder.apply(data);
		} else {
			eventsReactor.notify(read.getObject(), Event.wrap(data));
		}

		return data.remaining() > 0;
	}

	public void notifyRead(Object obj) {
		eventsReactor.notify(read.getObject(), (Event.class.isInstance(obj) ? (Event) obj : Event.wrap(obj)));
	}

	public void notifyError(Throwable throwable) {
		eventsReactor.notify(throwable.getClass(), Event.wrap(throwable));
	}

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write, as a {@link Buffer}.
	 * @param onComplete The callback to invoke when the write is complete.
	 */
	protected void write(Buffer data, Deferred<Void, Promise<Void>> onComplete, boolean flush) {
		write(data.byteBuffer(), onComplete, flush);
	}

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write.
	 * @param onComplete The callback to invoke when the write is complete.
	 * @param flush      whether to flush the underlying IO channel
	 */
	protected abstract void write(ByteBuffer data, Deferred<Void, Promise<Void>> onComplete, boolean flush);

	/**
	 * Subclasses must implement this method to perform the actual IO of writing data to the connection.
	 *
	 * @param data       The data to write.
	 * @param onComplete The callback to invoke when the write is complete.
	 * @param flush      whether to flush the underlying IO channel
	 */
	protected abstract void write(Object data, Deferred<Void, Promise<Void>> onComplete, boolean flush);

	/**
	 * Subclasses must implement this method to perform IO flushes.
	 */
	protected abstract void flush();

	protected Environment getEnvironment() {
		return env;
	}

	protected Reactor getEventsReactor() {
		return eventsReactor;
	}

	protected Reactor getIoReactor() {
		return ioReactor;
	}

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
				eventsReactor.notify(t.getClass(), Event.wrap(t));
				if (null != onComplete) {
					onComplete.accept(t);
				}
			}
		}
	}

}
