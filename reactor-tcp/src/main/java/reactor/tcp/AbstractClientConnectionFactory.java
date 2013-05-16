/*
 * Copyright 2002-2013 the original author or authors.
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

import java.net.Socket;

/**
 * Abstract class for client connection factories; client connection factories
 * establish outgoing connections.
 * @author Gary Russell
 * @since 2.0
 *
 */
public abstract class AbstractClientConnectionFactory extends ConnectionFactorySupport {

	private TcpConnectionSupport theConnection;

	/**
	 * Constructs a factory that will established connections to the host and port.
	 * @param host The host.
	 * @param port The port.
	 */
	public AbstractClientConnectionFactory(String host, int port) {
		super(host, port);
	}

	/**
	 * Obtains a connection - if {@link #setSingleUse(boolean)} was called with
	 * true, a new connection is returned; otherwise a single connection is
	 * reused for all requests while the connection remains open.
	 */
	public TcpConnectionSupport getConnection() throws Exception {
		this.checkActive();
		if (this.isSingleUse()) {
			return obtainConnection();
		} else {
			synchronized(this) {
				TcpConnectionSupport connection = obtainConnection();
				this.setTheConnection(connection);
				return connection;
			}
		}
	}

	protected abstract TcpConnectionSupport obtainConnection() throws Exception;

	/**
	 * Transfers attributes such as (de)serializers, singleUse etc to a new connection.
	 * When the connection factory has a reference to a TCPListener (to read
	 * responses), or for single use connections, the connection is executed.
	 * Single use connections need to read from the connection in order to
	 * close it after the socket timeout.
	 * @param connection The new connection.
	 * @param socket The new socket.
	 */
	protected void initializeConnection(TcpConnectionSupport connection, Socket socket) {
		TcpListener listener = this.getListener();
		if (listener != null) {
			connection.registerListener(listener);
		}
		connection.setSingleUse(this.isSingleUse());
		connection.publishConnectionOpenEvent();
	}

	/**
	 * @param theConnection the theConnection to set
	 */
	protected void setTheConnection(TcpConnectionSupport theConnection) {
		this.theConnection = theConnection;
	}

	/**
	 * @return the theConnection
	 */
	protected TcpConnectionSupport getTheConnection() {
		return theConnection;
	}

	/**
	 * Force close the connection and null the field if it's
	 * a shared connection.
	 * @param connection
	 */
	public void forceClose(TcpConnection connection) {
		if (this.theConnection == connection) {
			this.theConnection = null;
		}
		connection.close();
	}

}
