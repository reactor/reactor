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
import reactor.fn.Supplier;
import reactor.tcp.codec.Codec;

/**
 * A {@literal TcpServerReactor} is a {@link Reactor} that acts as a TCP server. It listens on a configurable
 * report, decoding the requests which it receives, and passing the decoded request to any {@link
 * #onRequest(Consumer) registered Consumers}.
 *
 * @author Andy Wilkinson
 */
public class TcpServerReactor<T> extends Reactor implements Lifecycle<TcpServerReactor<T>> {

	private final Object requestKey = new Object();

	private final Selector requestSelector = $(requestKey);

	private final Object monitor = new Object();

	private final TcpNioServerConnectionFactory<T> server;

	private boolean alive = false;

	/**
	 * Creates a new {@literal TcpServerReactor} that will listen on the given {@literal port} and will use the given
	 * {@literal codec} to decode the data it receives.
	 *
	 * @param port The port to listen on
	 * @param codecSupplier The codec to use to decode incoming data
	 */
	public TcpServerReactor(int port, Supplier<Codec<T>> codecSupplier) {
		server = new TcpNioServerConnectionFactory<T>(port, codecSupplier);
		server.registerListener(new TcpListener<T>() {
			@Override
			public void onDecode(T result, final TcpConnection<T> connection) {
				Event<T> event = Fn.event(result);

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
	public <E extends Event<T>> Registration<Consumer<E>> onRequest(Consumer<E> consumer) {
		return on(requestSelector, consumer);
	}

	@Override
	public TcpServerReactor<T> destroy() {
		return this;
	}

	@Override
	public TcpServerReactor<T> stop() {
		synchronized(monitor) {
			if (!alive) {
				server.stop();
				alive = false;
			}
		}
		return this;
	}

	@Override
	public TcpServerReactor<T> start() {
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
