package reactor.tcp;

import reactor.C;
import reactor.Fn;
import reactor.R;
import reactor.core.Composable;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Function;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.selector.Selector;
import reactor.fn.tuples.Tuple2;
import reactor.io.Buffer;

import static reactor.Fn.$;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractTcpConnection<IN, OUT> implements TcpConnection<IN, OUT> {

	private final long                     created = System.currentTimeMillis();
	private final Tuple2<Selector, Object> read    = $();
	private final Function<Buffer, IN>  decoder;
	private final Function<OUT, Buffer> encoder;

	protected final Dispatcher  ioDispatcher;
	protected final Reactor     ioReactor;
	protected final Reactor     eventsReactor;
	protected final Environment env;

	protected AbstractTcpConnection(Environment env,
																	Function<Buffer, IN> decoder,
																	Function<OUT, Buffer> encoder,
																	Dispatcher ioDispatcher,
																	Reactor eventsReactor) {
		this.env = env;
		this.decoder = decoder;
		this.encoder = encoder;
		this.ioDispatcher = ioDispatcher;
		this.ioReactor = R.reactor()
											.using(env)
											.using(ioDispatcher)
											.get();
		this.eventsReactor = eventsReactor;
	}

	public long getCreated() {
		return created;
	}

	public Object getReadKey() {
		return read.getT2();
	}

	@Override
	public void close() {
		eventsReactor.getConsumerRegistry().unregister(read.getT2());
	}


	@Override
	public Composable<IN> in() {
		Composable<IN> c = C.<IN>defer()
												.using(env)
												.using(eventsReactor.getDispatcher())
												.get();
		consume(c);
		return c;
	}

	@Override
	public Composable<OUT> out() {
		return C.<OUT>defer()
						.using(env)
						.using(eventsReactor.getDispatcher())
						.get();
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
	public Composable<OUT> receive(final Function<IN, OUT> fn) {
		final Composable<OUT> c = out();
		consume(new Consumer<IN>() {
			@Override
			public void accept(IN in) {
				OUT out = fn.apply(in);
				send(c);
				c.accept(out);
			}
		});
		return c;
	}

	@Override
	public TcpConnection<IN, OUT> send(Composable<OUT> data) {
		data.consume(new Consumer<OUT>() {
			@Override
			public void accept(OUT out) {
				send(out, null);
			}
		});
		return this;
	}

	@Override
	public TcpConnection<IN, OUT> send(OUT data) {
		return send(data, null);
	}

	@Override
	public TcpConnection<IN, OUT> send(OUT data, final Consumer<Boolean> onComplete) {
		Fn.schedule(
				new Consumer<OUT>() {
					@Override
					public void accept(OUT data) {
						if (null != encoder) {
							Buffer bytes;
							while (null != (bytes = encoder.apply(data)) && bytes.remaining() > 0) {
								write(bytes, onComplete);
							}
						} else {
							write(data, onComplete);
						}
					}
				},
				data,
				ioReactor
		);
		return this;
	}

	public boolean read(Buffer data) {
		IN in;
		if (null != decoder) {
			while ((null != data.byteBuffer() && data.byteBuffer().hasRemaining()) && null != (in = decoder.apply(data))) {
				eventsReactor.notify(read.getT2(), Event.wrap(in));
			}
		} else {
			eventsReactor.notify(read.getT2(), Event.wrap(data));
		}

		return data.remaining() > 0;
	}

	protected abstract void write(Buffer data, Consumer<Boolean> onComplete);

	protected abstract void write(Object data, Consumer<Boolean> onComplete);

}
