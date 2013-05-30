package reactor.tcp;

import static reactor.Fn.$;

import java.io.IOException;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.Fn;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Lifecycle;
import reactor.fn.Registration;
import reactor.fn.selector.Selector;
import reactor.fn.Supplier;
import reactor.tcp.codec.Codec;

/**
 * A {@literal TcpServerReactor} is a {@link Reactor} that acts as a TCP server. It listens on a configurable
 * port, decoding the requests which it receives, and passing the decoded request to any {@link
 * #onRequest(Consumer) registered Consumers}.
 *
 * @author Andy Wilkinson
 */
public class TcpServerReactor<T> extends Reactor implements Lifecycle<TcpServerReactor<T>> {

	private static final String HEADER_NAME_TCP_CONNECTION_ID = "reactor.tcp.connection.id";

	private final Object requestKey = new Object();

	private final Selector requestSelector = $(requestKey);

	private final Object monitor = new Object();

	private final TcpNioServerConnectionFactory<T> server;

	private final ReplySendingConsumer replyingConsumer = new ReplySendingConsumer();

	private final Object replyKey = new Object();

	private final Selector replySelector = $(replyKey);

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
		server.registerListener(new ConnectionAwareTcpListener<T>() {
			@Override
			public void onDecode(T result, final TcpConnection<T> connection) {
				Event<T> event = Fn.event(result);

				event.setReplyTo(replyKey);
				event.getHeaders().set(HEADER_NAME_TCP_CONNECTION_ID, connection.getConnectionId());



				TcpServerReactor.this.notify(requestKey, event);
			}

			@Override
			public void newConnection(TcpConnection<T> connection) {
				replyingConsumer.storeConnection(connection);

			}

			@Override
			public void connectionClosed(TcpConnection<T> connection) {
				replyingConsumer.removeConnection(connection);
			}
		});


		on(replySelector, replyingConsumer);
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

	private class ReplySendingConsumer implements Consumer<Event<Object>> {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		private final WeakHashMap<String, TcpConnection<?>> connections = new WeakHashMap<String, TcpConnection<?>>();

		private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

		private final Lock readLock = readWriteLock.readLock();

		private final Lock writeLock = readWriteLock.writeLock();

		@Override
		public void accept(Event<Object> event) {
			try {
				TcpConnection<?> connection = getConnection(event.getHeaders().get(HEADER_NAME_TCP_CONNECTION_ID));
				if (connection != null) {
					if (event.getData() instanceof byte[]) {
						connection.send((byte[]) event.getData());
					} else {
						connection.send(event.getData());
					}
				} else {
					this.logger.warn("Connection not available to send response. Response '{}' has been dropped", event.getData());
				}
			} catch (IOException e) {
				this.logger.warn("Failed to send response '{}'", event.getData(), e);
			}
		}

		private TcpConnection<?> getConnection(String id) {
			readLock.lock();
			try {
				return this.connections.get(id);
			} finally {
				readLock.unlock();
			}
		}

		private void storeConnection(TcpConnection<?> connection) {
			writeLock.lock();
			try {
				connections.put(connection.getConnectionId(), connection);
			} finally {
				writeLock.unlock();
			}
		}

		private void removeConnection(TcpConnection<?> connection) {
			writeLock.lock();
			try {
				connections.remove(connection.getConnectionId());
			} finally {
				writeLock.unlock();
			}
		}
	}
}
