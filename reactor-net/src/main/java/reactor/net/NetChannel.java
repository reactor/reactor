package reactor.net;

import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.batch.BatchConsumer;

/**
 * {@code NetChannel} implementations handle interacting with the client.
 *
 * @author Jon Brisbin
 */
public interface NetChannel<IN, OUT> {

	/**
	 * {@link reactor.core.composable.Stream} of incoming decoded data.
	 *
	 * @return input {@link reactor.core.composable.Stream}
	 */
	Stream<IN> in();

	/**
	 * {@link reactor.function.batch.BatchConsumer} for efficiently data to the peer.
	 *
	 * @return output {@link reactor.function.batch.BatchConsumer}
	 */
	BatchConsumer<OUT> out();

	/**
	 * When an error of the given type occurs, handle it with the given {@link reactor.function.Consumer}.
	 *
	 * @param type
	 * 		type of error
	 * @param onError
	 * 		error handler
	 * @param <T>
	 * 		type of the exception
	 *
	 * @return {@literal this}
	 */
	<T extends Throwable> NetChannel<IN, OUT> when(Class<T> type, Consumer<T> onError);

	/**
	 * Efficiently consume incoming decoded data.
	 *
	 * @param consumer
	 * 		the incoming data {@link reactor.function.Consumer}
	 *
	 * @return {@literal this}
	 */
	NetChannel<IN, OUT> consume(Consumer<IN> consumer);

	/**
	 * Handle incoming data and return the response.
	 *
	 * @param fn
	 * 		request handler
	 *
	 * @return {@literal this}
	 */
	NetChannel<IN, OUT> receive(Function<IN, OUT> fn);

	/**
	 * Send data to the peer that passes through the given {@link reactor.core.composable.Stream}.
	 *
	 * @param data
	 * 		the {@link reactor.core.composable.Stream} of data to monitor
	 *
	 * @return {@literal this}
	 */
	NetChannel<IN, OUT> send(Stream<OUT> data);

	/**
	 * Send data to the peer.
	 *
	 * @param data
	 * 		the data to send
	 *
	 * @return a {@link reactor.core.composable.Promise} indicating when the send operation has completed
	 */
	Promise<Void> send(OUT data);

	/**
	 * Send data to the peer.
	 *
	 * @param data
	 * 		the data to send
	 *
	 * @return {@literal this}
	 */
	NetChannel<IN, OUT> sendAndForget(OUT data);

	/**
	 * Send data to the peer and expect a response.
	 *
	 * @param data
	 * 		the data to send
	 *
	 * @return a {@link reactor.core.composable.Promise} representing the response from the peer
	 */
	Promise<IN> sendAndReceive(OUT data);

	/**
	 * Close this {@literal NetChannel}.
	 */
	Promise<Void> close();

	/**
	 * Close this {@link reactor.net.NetChannel} and invoke the given {@link reactor.function.Consumer} when closed.
	 *
	 * @param onClose
	 */
	void close(Consumer<Void> onClose);

	/**
	 * Assign event handlers to certain channel lifecycle events.
	 *
	 * @return
	 */
	ConsumerSpec on();

	/**
	 * Spec class for assigning multiple event handlers on a channel.
	 */
	public static interface ConsumerSpec {
		/**
		 * Assign a {@link Runnable} to be invoked when the channel is closed.
		 *
		 * @param onClose
		 * 		the close event handler
		 *
		 * @return {@literal this}
		 */
		ConsumerSpec close(Runnable onClose);

		/**
		 * Assign a {@link Runnable} to be invoked when reads have become idle for the given timeout.
		 *
		 * @param idleTimeout
		 * 		the idle timeout
		 * @param onReadIdle
		 * 		the idle timeout handler
		 *
		 * @return {@literal this}
		 */
		ConsumerSpec readIdle(long idleTimeout, Runnable onReadIdle);

		/**
		 * Assign a {@link Runnable} to be invoked when writes have become idle for the given timeout.
		 *
		 * @param idleTimeout
		 * 		the idle timeout
		 * @param onWriteIdle
		 * 		the idle timeout handler
		 *
		 * @return {@literal this}
		 */
		ConsumerSpec writeIdle(long idleTimeout, Runnable onWriteIdle);
	}

}
