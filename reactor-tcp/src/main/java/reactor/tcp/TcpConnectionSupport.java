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
import java.net.InetAddress;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.support.Assert;
import reactor.tcp.TcpConnectionEvent.TcpConnectionEventType;

/**
 * Base class for TcpConnections. TcpConnections are established by
 * client connection factories (outgoing) or server connection factories
 * (incoming).
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public abstract class TcpConnectionSupport implements TcpConnection {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private final ConnectionFactorySupport connectionFactory;

	private volatile TcpListener listener;

	private volatile boolean singleUse;

	private final boolean server;

	private volatile String connectionId;

	private final AtomicLong sequence = new AtomicLong();

	private volatile String hostName = "unknown";

	private volatile String hostAddress = "unknown";

	private volatile String connectionFactoryName = "unknown";

	private final AtomicBoolean closePublished = new AtomicBoolean();

	private volatile long lastRead;

	private volatile long lastSend;

	/**
	 * Creates a {@link TcpConnectionSupport} object and publishes a {@link TcpConnectionEvent}
	 * with {@link TcpConnectionEventType#OPEN}, if so configured.
	 * @param socket the underlying socket.
	 * @param server true if this connection is a server connection
	 * @param lookupHost true if reverse lookup of the host name should be performed,
	 * otherwise, the ip address will be used for identification purposes.
	 * @param applicationEventPublisher the publisher to which OPEN, CLOSE and EXCEPTION events will
	 * be sent; may be null if event publishing is not required.
	 * @param connectionFactoryName the name of the connection factory creating this connection; used
	 * during event publishing, may be null, in which case "unknown" will be used.
	 */
	public TcpConnectionSupport(Socket socket, boolean server, boolean lookupHost,
			ConnectionFactorySupport connectionFactory) {
		this.connectionFactory = connectionFactory;
		this.server = server;
		InetAddress inetAddress = socket.getInetAddress();
		if (inetAddress != null) {
			this.hostAddress = inetAddress.getHostAddress();
			if (lookupHost) {
				this.hostName = inetAddress.getHostName();
			} else {
				this.hostName = this.hostAddress;
			}
		}
		int port = socket.getPort();
		this.connectionId = this.hostName + ":" + port + ":" + UUID.randomUUID().toString();
		if (connectionFactoryName != null) {
			this.connectionFactoryName = connectionFactoryName;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("New connection " + this);
		}
	}

	/**
	 * Closes this connection.
	 */
	@Override
	public void close() {
		// close() may be called multiple times; only publish once
		if (!this.closePublished.getAndSet(true)) {
			this.publishConnectionCloseEvent();
		}
	}

	/**
	 * If we have been intercepted, propagate the close from the outermost interceptor;
	 * otherwise, just call close().
	 */
	protected void closeConnection() {
		close();
	}

	/**
	 * Sets the listener that will receive incoming Messages.
	 * @param listener The listener.
	 */
	public void registerListener(TcpListener listener) {
		this.listener = listener;
	}

	/**
	 * @return the listener
	 */
	@Override
	public TcpListener getListener() {
		return this.listener;
	}

	/**
	 * @param singleUse true if this socket is to used once and
	 * discarded.
	 */
	public void setSingleUse(boolean singleUse) {
		this.singleUse = singleUse;
	}

	/**
	 *
	 * @return True if connection is used once.
	 */
	@Override
	public boolean isSingleUse() {
		return this.singleUse;
	}

	@Override
	public boolean isServer() {
		return server;
	}

	@Override
	public long incrementAndGetConnectionSequence() {
		return this.sequence.incrementAndGet();
	}

	@Override
	public String getHostAddress() {
		return this.hostAddress;
	}

	@Override
	public String getHostName() {
		return this.hostName;
	}

	@Override
	public String getConnectionId() {
		return this.connectionId;
	}

	protected ConnectionFactorySupport getConnectionFactory() {
		return connectionFactory;
	}

	protected void publishConnectionOpenEvent() {
		TcpConnectionEvent event = new TcpConnectionEvent(this, TcpConnectionEventType.OPEN,
				this.connectionFactoryName);
		doPublish(event);
	}

	protected void publishConnectionCloseEvent() {
		TcpConnectionEvent event = new TcpConnectionEvent(this, TcpConnectionEventType.CLOSE,
				this.connectionFactoryName);
		doPublish(event);
	}

	protected void publishConnectionExceptionEvent(Throwable t) {
		TcpConnectionEvent event = new TcpConnectionEvent(this, t,
				this.connectionFactoryName);
		doPublish(event);
	}

	/**
	 * Allow interceptors etc to publish events, perhaps subclasses of
	 * TcpConnectionEvent. The event source must be this connection.
	 * @param event the event to publish.
	 */
	public void publishEvent(TcpConnectionEvent event) {
		Assert.isTrue(event.getData() == this, "Can only publish events with this as the source");
		this.doPublish(event);
	}

	private void doPublish(TcpConnectionEvent event) {
		if (this.listener instanceof ConnectionAwareTcpListener) {
			ConnectionAwareTcpListener listener = (ConnectionAwareTcpListener) this.listener;
			if (event.getType() == TcpConnectionEventType.OPEN) {
				listener.newConnection(this);
			}
			else if (event.getType() == TcpConnectionEventType.CLOSE) {
				listener.connectionClosed(this);
			}
		}
		try {
			// TODO Publish the event to a reactor
			logger.info("Publishing event {}", event);
		}
		catch (Exception e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Failed to publish " + event, e);
			}
			else if (logger.isWarnEnabled()) {
				logger.warn("Failed to publish " + event + ":" + e.getMessage());
			}
		}
	}

	@Override
	public void send(byte[] bytes) throws IOException {
		this.send(bytes, 0, bytes.length);
	}

	/**
	 *
	 * @return Time of last read.
	 */
	public long getLastRead() {
		return lastRead;
	}

	/**
	 *
	 * @param lastRead The time of the last read.
	 */
	public void setLastRead(long lastRead) {
		this.lastRead = lastRead;
	}

	/**
	 * @return the time of the last send
	 */
	public long getLastSend() {
		return lastSend;
	}

	/**
	 * Close the socket due to timeout.
	 */
	void timeout() {
		this.closeConnection();
	}
}
