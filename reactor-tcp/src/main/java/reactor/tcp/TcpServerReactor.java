package reactor.tcp;

import static reactor.Fn.$;

import java.io.IOException;

import reactor.Fn;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Lifecycle;
import reactor.fn.Registration;
import reactor.fn.Selector;
import reactor.tcp.codec.Codec;
import reactor.tcp.codec.DecoderResult;
import reactor.tcp.codec.LineFeedCodec;

/**
 * A {@literal TcpServerReactor} is a {@link Reactor} that acts as a TCP server. It listens on a configurable
 * report, decoding the requests which it receives, and passing the decoded request to any {@link
 * #onRequest(Consumer) registered Consumers}.
 *
 * @author Andy Wilkinson
 */
public class TcpServerReactor extends Reactor implements Lifecycle<TcpServerReactor> {

	private final Object requestKey = new Object();

	private final Selector requestSelector = $(requestKey);

	private final Object monitor = new Object();

	private final TcpNioServerConnectionFactory server;

	private boolean alive = false;

	/**
	 * Creates a new {@literal TcpServerReactor} that will listen on the given {@literal port} and will use the default
	 * {@link LineFeedCodec} to decode the data it receives.
	 *
	 * @param port The port to listen on
	 */
	public TcpServerReactor(int port) {
		this(port, new LineFeedCodec());
	}

	/**
	 * Creates a new {@literal TcpServerReactor} that will listen on the given {@literal port} and will use the given
	 * {@literal codec} to decode the data it receives.
	 *
	 * @param port The port to listen on
	 * @param codec The codec to use to decode incoming data
	 */
	public TcpServerReactor(int port, Codec codec) {
		server = new TcpNioServerConnectionFactory(port);
		server.setCodec(codec);
		server.registerListener(new TcpListener() {
			@Override
			public void onDecode(DecoderResult result, final TcpConnection connection) {
				Event<DecoderResult> event = Fn.event(result);

				Object replyToKey = new Object();
				event.setReplyTo(replyToKey);

				TcpServerReactor.this.on($(replyToKey), new Consumer<Event<Object>>() {
					@Override
					public void accept(Event<Object> t) {
						try {
							if (t.getData() instanceof byte[]) {
								connection.send((byte[]) t.getData());
							} else {
								connection.send(t.getData());
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}).cancelAfterUse();

				TcpServerReactor.this.notify(requestKey, event);
			}
		});
	}

	/**
	 * Registers the {@literal Consumer} to be notified of each request that is received.
	 *
	 * @param consumer The {@literal Consumer} that will {@link Consumer#accept(Object)} each request
	 *
	 * @return The registration
	 */
	public <T, E extends Event<T>> Registration<Consumer<E>> onRequest(Consumer<E> consumer) {
		return on(requestSelector, consumer);
	}

	@Override
	public TcpServerReactor destroy() {
		return this;
	}

	@Override
	public TcpServerReactor stop() {
		synchronized(monitor) {
			if (!alive) {
				server.stop();
				alive = false;
			}
		}
		return this;
	}

	@Override
	public TcpServerReactor start() {
		synchronized(monitor) {
			if (!alive) {
				alive = true;
				server.start();
			}
		}
		return this;
	}

	/**
	 * Indicates whether or not the {@code TcpServerReactor} is alive. A {@code TcpServerReactor} is considered to be alive once
	 * it's been {@link #start started} and its underlying TCP server is listening and therefore ready to accept request.
	 *
	 * @boolean {@code true} if started and ready to accept requests, otherwise {@code false}.
	 */
	@Override
	public boolean isAlive() {
		synchronized(monitor) {
			return alive && server.isListening();
		}
	}
}
