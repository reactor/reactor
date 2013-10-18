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

import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.function.Consumer;
import reactor.function.Function;

import java.net.InetSocketAddress;

/**
 * Implementations of this class provide functionality for reading and writing to TCP connections.
 *
 * @param <IN>
 * 		The type that will be received by this connection
 * @param <OUT>
 * 		The type that will be sent by this connection
 *
 * @author Jon Brisbin
 */
public interface TcpConnection<IN, OUT> {

	/**
	 * Close this connection.
	 */
	void close();

	/**
	 * Determines whether this connection can have data consumed from it.
	 *
	 * @return {@literal true} if the connection is consumable, {@literal false} otherwise.
	 */
	boolean consumable();

	/**
	 * Determines whether this connection can have data written to it.
	 *
	 * @return {@literal true} if the connection is writable, {@literal false} otherwise.
	 */
	boolean writable();

	/**
	 * Get the {@link InetSocketAddress} of the client.
	 *
	 * @return The client's address.
	 */
	InetSocketAddress remoteAddress();

	/**
	 * Get the {@link Stream} of data coming in.
	 *
	 * @return The incoming data, as a {@link Stream}.
	 */
	Stream<IN> in();

	/**
	 * Get a {@link Consumer} for output. To send data to the output using this {@link Consumer}, the caller invokes the
	 * {@link Consumer#accept} method.
	 *
	 * @return An output {@link Consumer}.
	 */
	Consumer<OUT> out();

	/**
	 * Set an error {@link Consumer} for errors that happen on the connection.
	 *
	 * @param errorType
	 * 		The type of error to handle.
	 * @param errorConsumer
	 * 		The {@link Consumer} when the given error occurs.
	 * @param <T>
	 * 		The type of the error.
	 *
	 * @return {@literal this}
	 */
	<T extends Throwable> TcpConnection<IN, OUT> when(Class<T> errorType, Consumer<T> errorConsumer);

	/**
	 * Set a callback for consuming decoded data.
	 *
	 * @param consumer
	 * 		The data {@link Consumer}.
	 *
	 * @return {@literal this}
	 */
	TcpConnection<IN, OUT> consume(Consumer<IN> consumer);

	/**
	 * Use the given {@link Function} to handle incoming data, like in {@link #consume(reactor.function.Consumer)}, but
	 * this method expects the {@link Function} to return a response object.
	 *
	 * @param fn
	 * 		The data-consuming {@link Function}.
	 *
	 * @return {@literal this}
	 */
	TcpConnection<IN, OUT> receive(Function<IN, OUT> fn);

	/**
	 * Send data on this connection as a {@link Stream}. The implementation is expected to place a {@link Consumer} on
	 * this {@link Stream} to handle data coming in.
	 *
	 * @param data
	 * 		The outgoing data as a {@link Stream}.
	 *
	 * @return {@literal this}
	 */
	TcpConnection<IN, OUT> send(Stream<OUT> data);

	/**
	 * Send data on this connection. The current codec (if any) will be used to encode the data to a {@link
	 * reactor.io.Buffer}. If the send fails for some reason, the returned {@link Promise} will be fulfilled with an
	 * error
	 * indicating the cause of the failure.
	 *
	 * @param data
	 * 		The outgoing data.
	 *
	 * @return A {@link reactor.core.composable.Promise} that will be completed on successful send to the peer.
	 */
	Promise<Void> send(OUT data);

	/**
	 * Send data on this connection in a fire-and-forget manner. If an error occurs during send, it will be reported in
	 * the usual manner and can be handle by calling {@link #when(Class, reactor.function.Consumer)}.
	 *
	 * @param data
	 *
	 * @return
	 */
	TcpConnection<IN, OUT> sendAndForget(OUT data);

	/**
	 * Send data on this connection and return a {@link reactor.core.composable.Promise} that will be fulfilled by the
	 * response from the peer or by an exception generated during the attempt to send.
	 *
	 * @param data
	 * 		The outgoing data.
	 *
	 * @return A {@link reactor.core.composable.Promise} that will be fulfilled by the peer's response or an error.
	 */
	Promise<IN> sendAndReceive(OUT data);

	/**
	 * Provide the caller with a spec-style configuration object that allows a user to attach multiple event handlers to
	 * the connection.
	 *
	 * @return
	 */
	ConsumerSpec on();

	/**
	 * Spec class for assigning multiple event handlers on a connection.
	 *
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 */
	public static interface ConsumerSpec<IN, OUT> {
		/**
		 * Assign a {@link Runnable} to be invoked when the connection is closed.
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
