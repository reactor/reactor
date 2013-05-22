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

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import reactor.fn.Supplier;
import reactor.support.Assert;
import reactor.tcp.codec.Codec;

/**
 * Base class for all server connection factories. Server connection factories
 * listen on a port for incoming connections and create new TcpConnection objects
 * for each new connection.
 *
 * @author Gary Russell
 */
public abstract class AbstractServerConnectionFactory<T>
		extends ConnectionFactorySupport<T> implements Runnable {

	private static final int DEFAULT_BACKLOG = 5;

	private volatile boolean listening;

	private volatile String localAddress;

	private volatile int backlog = DEFAULT_BACKLOG;

	private volatile boolean shuttingDown;


	/**
	 * The port on which the factory will listen.
	 * @param port
	 */
	public AbstractServerConnectionFactory(int port, Supplier<Codec<T>> codecSupplier) {
		super(port, codecSupplier);
	}

	@Override
	public ConnectionFactory start() {
		synchronized (this.lifecycleMonitor) {
			if (!this.isActive()) {
				super.start();
				this.setActive(true);
				this.shuttingDown = false;
				this.getTaskExecutor().execute(this);
			}
		}

		return this;
	}

	/**
	 * @param listening the listening to set
	 */
	protected void setListening(boolean listening) {
		this.listening = listening;
	}


	/**
	 *
	 * @return true if the server is listening on the port.
	 */
	public boolean isListening() {
		return listening;
	}

	protected boolean isShuttingDown() {
		return shuttingDown;
	}

	/**
	 * Transfers attributes such as (de)serializer, singleUse etc to a new connection.
	 * For single use sockets, enforces a socket timeout (default 10 seconds).
	 * @param connection The new connection.
	 * @param socket The new socket.
	 */
	protected void initializeConnection(TcpConnectionSupport<T> connection, Socket socket) {
		TcpListener<T> listener = this.getListener();
		if (listener != null) {
			connection.registerListener(listener);
		}
		connection.setSingleUse(this.isSingleUse());
		/*
		 * If we are configured
		 * for single use; need to enforce a timeout on the socket so we will close
		 * if the client connects, but sends nothing. (Protect against DoS).
		 * Behavior can be overridden by explicitly setting the timeout to zero.
		 */
		if (this.isSingleUse() && this.getSoTimeout() < 0) {
			try {
				socket.setSoTimeout(DEFAULT_REPLY_TIMEOUT);
			}
			catch (SocketException e) {
				logger.error("Error setting default reply timeout", e);
			}
		}
		connection.publishConnectionOpenEvent();

	}

	protected void postProcessServerSocket(ServerSocket serverSocket) {
		this.getTcpSocketSupport().postProcessServerSocket(serverSocket);
	}

	/**
	 *
	 * @return the localAddress
	 */
	public String getLocalAddress() {
		return localAddress;
	}

	/**
	 * Used on multi-homed systems to enforce the server to listen
	 * on a specfic network address instead of all network adapters.
	 * @param localAddress the ip address of the required adapter.
	 */
	public void setLocalAddress(String localAddress) {
		this.localAddress = localAddress;
	}


	/**
	 * The number of sockets in the server connection backlog.
	 * @return The backlog.
	 */
	public int getBacklog() {
		return backlog;
	}

	/**
	 * The number of sockets in the connection backlog. Default 5;
	 * increase if you expect high connection rates.
	 * @param backlog
	 */
	public void setBacklog(int backlog) {
		Assert.isTrue(backlog >= 0, "You cannot set backlog negative");
		this.backlog = backlog;
	}

}
