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

import reactor.core.Stream;
import reactor.fn.Consumer;
import reactor.fn.Function;

import java.net.InetSocketAddress;

/**
 * Implementations of this class provide functionality for reading and writing to TCP connections.
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
	 * {@link Consumer#accept(OUT)} method.
	 *
	 * @return An output {@link Consumer}.
	 */
	Consumer<OUT> out();

	/**
	 * Set a callback for consuming decoded data.
	 *
	 * @param consumer The data {@link Consumer}.
	 * @return {@literal this}
	 */
	TcpConnection<IN, OUT> consume(Consumer<IN> consumer);

	/**
	 * Use the given {@link Function} to handle incoming data, like in {@link #consume(reactor.fn.Consumer)}, but this
	 * method expects the {@link Function} to return a response object.
	 *
	 * @param fn The data-consuming {@link Function}.
	 * @return {@literal this}
	 */
	TcpConnection<IN, OUT> receive(Function<IN, OUT> fn);

	/**
	 * Send data on this connection as a {@link Stream}. The implementation is expected to place a {@link Consumer} on this
	 * {@link Stream} to handle data coming in.
	 *
	 * @param data The outgoing data as a {@link Stream}.
	 * @return {@literal this}
	 */
	TcpConnection<IN, OUT> send(Stream<OUT> data);

	/**
	 * Send data on this connection. The current codec (if any) will be used to encode the data to a {@link
	 * reactor.io.Buffer}.
	 *
	 * @param data The outgoing data.
	 * @return {@literal this}
	 */
	TcpConnection<IN, OUT> send(OUT data);

	/**
	 * Send data on this connection. The current codec (if any) will be used to encode the data to a {@link
	 * reactor.io.Buffer}. The given callback will be invoked when the write has completed.
	 *
	 * @param data       The outgoing data.
	 * @param onComplete The callback to invoke when the write is complete.
	 * @return {@literal this}
	 */
	TcpConnection<IN, OUT> send(OUT data, Consumer<Boolean> onComplete);

}
