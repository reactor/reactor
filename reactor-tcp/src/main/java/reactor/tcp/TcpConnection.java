/*
 * Copyright 2001-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.tcp;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

/**
 * An abstraction over {@link Socket} and {@link SocketChannel} that
 * sends {@link Message} objects by serializing the payload
 * and streaming it to the destination. Requires a {@link TcpListener}
 * to receive incoming messages.
 *
 * @author Gary Russell
 *
 */
public interface TcpConnection {

	/**
	 * Closes the connection.
	 */
	void close();

	/**
	 * @return true if the connection is open.
	 */
	boolean isOpen();

	/**
	 * Encodes and sends the message.
	 * @param message The message
	 * @throws Exception
	 */
	void send(byte[] bytes) throws IOException;

	void send(byte[] bytes, int offset, int length) throws IOException;

	void send(Object object) throws IOException;

	/**
	 * @return the host name
	 */
	String getHostName();

	/**
	 * @return the host address
	 */
	String getHostAddress();

	/**
	 * @return the port
	 */
	int getPort();

	/**
	 * @return a string uniquely representing a connection.
	 */
	String getConnectionId();

	/**
	 *
	 * @return True if connection is used once.
	 */
	boolean isSingleUse();

	/**
	 *
	 * @return True if connection is used once.
	 */
	boolean isServer();

	/**
	 * @return this connection's listener
	 */
	TcpListener getListener();

	/**
	 * @return the next sequence number for a message received on this socket
	 */
	long incrementAndGetConnectionSequence();

}
