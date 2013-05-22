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

import static reactor.Fn.$;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.Fn;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Supplier;
import reactor.support.Assert;
import reactor.tcp.codec.Codec;

/**
 * Base class for all connection factories.
 *
 * @author Gary Russell
 *
 */
public abstract class ConnectionFactorySupport<T> implements ConnectionFactory {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected static final int DEFAULT_REPLY_TIMEOUT = 10000;

	protected static final Object READ_KEY = new Object();

	protected static final reactor.fn.Selector READ = $(READ_KEY);

	protected static final Object WRITE_KEY = new Object();

	protected static final reactor.fn.Selector WRITE = $(WRITE_KEY);

	protected static final Object HARVEST_KEY = new Object();

	protected static final reactor.fn.Selector HARVEST = $(HARVEST_KEY);

	protected static final Object DECODE_KEY = new Object();

	protected static final reactor.fn.Selector DECODE = $(DECODE_KEY);

	private final Reactor ioReactor = new Reactor();

	private final Supplier<Codec<T>> codecSupplier;

	private volatile Selector ioSelector;

	private volatile String host;

	private volatile int port;

	private volatile TcpListener<T> listener;

	private volatile int soTimeout = -1;

	private volatile int soSendBufferSize;

	private volatile int soReceiveBufferSize;

	private volatile boolean soTcpNoDelay;

	private volatile int soLinger  = -1; // don't set by default

	private volatile boolean soKeepAlive;

	private volatile int soTrafficClass = -1; // don't set by default

	private volatile Executor taskExecutor;

	private volatile boolean privateExecutor;

	private volatile boolean singleUse;

	private volatile boolean active;

	private volatile boolean lookupHost = true;

	private volatile List<TcpConnectionSupport<T>> connections = new LinkedList<TcpConnectionSupport<T>>();

	private volatile TcpSocketConfigurer tcpSocketSupport = new DefaultTcpSocketSupport();

	protected final Object lifecycleMonitor = new Object();

	private volatile long nextCheckForClosedNioConnections;

	private volatile int nioHarvestInterval = DEFAULT_NIO_HARVEST_INTERVAL;

	private volatile String factoryName = "unknown";

	private static final int DEFAULT_NIO_HARVEST_INTERVAL = 2000;

	private final AtomicBoolean harvesting = new AtomicBoolean();

	private final BlockingQueue<TcpNioConnection<T>> writeOpsNeeded = new LinkedBlockingQueue<TcpNioConnection<T>>();

	public ConnectionFactorySupport(int port, Supplier<Codec<T>> codecSupplier) {
		this.port = port;
		this.codecSupplier = codecSupplier;
	}

	public ConnectionFactorySupport(String host, int port, Supplier<Codec<T>> codecSupplier) {
		Assert.notNull(host, "host must not be null");
		this.host = host;
		this.port = port;
		this.codecSupplier = codecSupplier;
	}

	/**
	 * @return the soTimeout
	 */
	public int getSoTimeout() {
		return soTimeout;
	}

	/**
	 * @param soTimeout the soTimeout to set
	 */
	public void setSoTimeout(int soTimeout) {
		this.soTimeout = soTimeout;
	}

	/**
	 * @return the soReceiveBufferSize
	 */
	public int getSoReceiveBufferSize() {
		return soReceiveBufferSize;
	}

	/**
	 * @param soReceiveBufferSize the soReceiveBufferSize to set
	 */
	public void setSoReceiveBufferSize(int soReceiveBufferSize) {
		this.soReceiveBufferSize = soReceiveBufferSize;
	}

	/**
	 * @return the soSendBufferSize
	 */
	public int getSoSendBufferSize() {
		return soSendBufferSize;
	}

	/**
	 * @param soSendBufferSize the soSendBufferSize to set
	 */
	public void setSoSendBufferSize(int soSendBufferSize) {
		this.soSendBufferSize = soSendBufferSize;
	}

	/**
	 * @return the soTcpNoDelay
	 */
	public boolean isSoTcpNoDelay() {
		return soTcpNoDelay;
	}

	/**
	 * @param soTcpNoDelay the soTcpNoDelay to set
	 */
	public void setSoTcpNoDelay(boolean soTcpNoDelay) {
		this.soTcpNoDelay = soTcpNoDelay;
	}

	/**
	 * @return the soLinger
	 */
	public int getSoLinger() {
		return soLinger;
	}

	/**
	 * @param soLinger the soLinger to set
	 */
	public void setSoLinger(int soLinger) {
		this.soLinger = soLinger;
	}

	/**
	 * @return the soKeepAlive
	 */
	public boolean isSoKeepAlive() {
		return soKeepAlive;
	}

	/**
	 * @param soKeepAlive the soKeepAlive to set
	 */
	public void setSoKeepAlive(boolean soKeepAlive) {
		this.soKeepAlive = soKeepAlive;
	}

	/**
	 * @return the soTrafficClass
	 */
	public int getSoTrafficClass() {
		return soTrafficClass;
	}

	/**
	 * @param soTrafficClass the soTrafficClass to set
	 */
	public void setSoTrafficClass(int soTrafficClass) {
		this.soTrafficClass = soTrafficClass;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @return the listener
	 */
	public TcpListener<T> getListener() {
		return listener;
	}

	/**
	 * Registers a TcpListener to receive messages after
	 * the payload has been converted from the input data.
	 * @param listener the TcpListener.
	 */
	public void registerListener(TcpListener<T> listener) {
		Assert.isNull(this.listener, this.getClass().getName() +
				" may only be used by one inbound adapter");
		this.listener = listener;
	}

	/**
	 * @param taskExecutor the taskExecutor to set
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * @return the singleUse
	 */
	public boolean isSingleUse() {
		return singleUse;
	}

	/**
	 * If true, sockets created by this factory will be used once.
	 * @param singleUse
	 */
	public void setSingleUse(boolean singleUse) {
		this.singleUse = singleUse;
	}


	/**
	 * If true, DNS reverse lookup is done on the remote ip address.
	 * Default true.
	 * @param lookupHost the lookupHost to set
	 */
	public void setLookupHost(boolean lookupHost) {
		this.lookupHost = lookupHost;
	}

	/**
	 * @return the lookupHost
	 */
	public boolean isLookupHost() {
		return lookupHost;
	}

	/**
	 * How often we clean up closed NIO connections if soTimeout is 0.
	 * Ignored when {@code soTimeout > 0} because the clean up
	 * process is run as part of the timeout handling.
	 * Default 2000 milliseconds.
	 * @param nioHarvestInterval The interval in milliseconds.
	 */
	public void setNioHarvestInterval(int nioHarvestInterval) {
		Assert.isTrue(nioHarvestInterval > 0, "NIO Harvest interval must be > 0");
		this.nioHarvestInterval = nioHarvestInterval;
	}


	public String getFactoryName() {
		return factoryName;
	}

	public void setFactoryName(String factoryName) {
		this.factoryName = factoryName;
	}

	protected Supplier<Codec<T>> getCodecSupplier() {
		return this.codecSupplier;
	}

	Selector getIoSelector() {
		return this.ioSelector;
	}

	@Override
	public ConnectionFactory start() {
		try {
			this.ioSelector = Selector.open();
		} catch (IOException ioe) {
			throw new RuntimeException();
		}

		this.ioReactor.on(HARVEST, new Consumer<Event<Object>>() {

			@Override
			public void accept(Event<Object> nullEvent) {
				doCheckTimeoutsAndHarvestClosed();
			}
		});
		return this;
	}

	void writeOpNeeded(TcpNioConnection<T> connection) {
		if (logger.isTraceEnabled()) {
			logger.trace("Write(s) needed for " + connection.getConnectionId());
		}
		this.writeOpsNeeded.add(connection);
		this.ioSelector.wakeup();
	}

	protected void setWriteOps() {
		TcpNioConnection<T> connection;
		while ((connection = this.writeOpsNeeded.poll()) != null) {
			SelectionKey key = connection.getSocketChannel().keyFor(this.ioSelector);
			if (key != null) {
				key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
			}
		}
	}

	/**
	 * Sets socket attributes on the socket.
	 * @param socket The socket.
	 * @throws SocketException
	 */
	protected void applySocketAttributes(Socket socket) throws SocketException {
		if (this.soTimeout >= 0) {
			socket.setSoTimeout(this.soTimeout);
		}
		if (this.soSendBufferSize > 0) {
			socket.setSendBufferSize(this.soSendBufferSize);
		}
		if (this.soReceiveBufferSize > 0) {
			socket.setReceiveBufferSize(this.soReceiveBufferSize);
		}
		socket.setTcpNoDelay(this.soTcpNoDelay);
		if (this.soLinger >= 0) {
			socket.setSoLinger(true, this.soLinger);
		}
		if (this.soTrafficClass >= 0) {
			socket.setTrafficClass(this.soTrafficClass);
		}
		socket.setKeepAlive(this.soKeepAlive);
		this.tcpSocketSupport.postProcessSocket(socket);
	}

	/**
	 * Closes the server.
	 */
	public abstract void close();

	/**
	 * Creates a taskExecutor (if one was not provided).
	 */
	protected Executor getTaskExecutor() {
		if (!this.active) {
			throw new RuntimeException("Connection Factory not started");
		}
		synchronized (this.lifecycleMonitor) {
			if (this.taskExecutor == null) {
				this.privateExecutor = true;
				this.taskExecutor = Executors.newCachedThreadPool();
			}
			return this.taskExecutor;
		}
	}

	/**
	 * Stops the server.
	 */
	@Override
	public ConnectionFactory stop() {
		this.active = false;
		this.close();
		synchronized (this.connections) {
			Iterator<TcpConnectionSupport<T>> iterator = this.connections.iterator();
			while (iterator.hasNext()) {
				TcpConnection<T> connection = iterator.next();
				connection.close();
				iterator.remove();
			}
		}
		synchronized (this.lifecycleMonitor) {
			if (this.privateExecutor) {
				ExecutorService executorService = (ExecutorService) this.taskExecutor;
				executorService.shutdown();
				try {
					if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
						logger.debug("Forcing executor shutdown");
						executorService.shutdownNow();
						if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
							logger.debug("Executor failed to shutdown");
						}
					}
				} catch (InterruptedException e) {
					executorService.shutdownNow();
					Thread.currentThread().interrupt();
				} finally {
					this.taskExecutor = null;
					this.privateExecutor = false;
				}
			}
		}
		if (logger.isInfoEnabled()) {
			logger.info("stopped " + this);
		}
		return this;
	}



	@Override
	public ConnectionFactory destroy() {
		return this;
	}

	@Override
	public final boolean isAlive() {
		return this.active;
	}

	protected void checkTimeoutsAndHarvestClosed() {
		if (this.harvesting.compareAndSet(false, true)) {
			this.ioReactor.notify(HARVEST_KEY, Fn.event(null));
		}
	}

	private void doCheckTimeoutsAndHarvestClosed() {
		long now = System.currentTimeMillis();
		if (this.soTimeout > 0 ||
				now >= this.nextCheckForClosedNioConnections) {
			this.nextCheckForClosedNioConnections = now + this.nioHarvestInterval;
			Iterator<TcpConnectionSupport<T>> it = connections.iterator();
			while (it.hasNext()) {
				TcpConnectionSupport<T> connection = it.next();
				if (this.soTimeout > 0 && connection.isOpen()) {
					if (now - connection.getLastRead() >= this.soTimeout) {
						/*
						 * For client connections, we have to wait for 2 timeouts if the last
						 * send was within the current timeout.
						 */
						if (!connection.isServer() &&
							now - connection.getLastSend() < this.soTimeout &&
							now - connection.getLastRead() < this.soTimeout * 2)
						{
							if (logger.isDebugEnabled()) {
								logger.debug("Skipping a connection timeout because we have a recent send "
										+ connection.getConnectionId());
							}
						}
						else {
							if (logger.isWarnEnabled()) {
								logger.warn("Timing out TcpNioConnection " +
											this.port + " : " +
										    connection.getConnectionId());
							}
							connection.publishConnectionExceptionEvent(new SocketTimeoutException("Timing out connection"));
							connection.timeout();
						}
					}
				}
			}
		}
		this.harvestClosedConnections();
		this.harvesting.set(false);
	}

	protected final void doSelect(final Selector selector) throws IOException {
		int soTimeout = this.getSoTimeout();
		int selectionCount = 0;
		try {
			selectionCount = selector.select(soTimeout < 0 ? 0 : soTimeout);
			if (logger.isTraceEnabled()) {
				this.traceSelections(selectionCount);
			}
			this.setWriteOps();
			this.handleSelections(selector);
		}
		catch (CancelledKeyException cke) {
			if (logger.isDebugEnabled()) {
				logger.debug("CancelledKeyException during Selector.select()");
			}
		}
	}

	private void handleSelections(final Selector selector) {
		Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
		long now = System.currentTimeMillis();
		checkTimeoutsAndHarvestClosed();
		while (iterator.hasNext()) {
			final SelectionKey key = iterator.next();
			iterator.remove();
			handleKey(now, key);
		}
	}

	@SuppressWarnings("unchecked")
	private void handleKey(long now, final SelectionKey key) {
		try {
			if (!key.isValid()) {
				logger.debug("Selection key no longer valid");
			}
			else {
				if (key.isReadable()) {
					key.interestOps(key.interestOps() - SelectionKey.OP_READ);
					TcpNioConnection<T> connection;
					connection = (TcpNioConnection<T>) key.attachment();
					connection.getConnectionReactor().notify(READ_KEY, Fn.event(key));
				}
				if (key.isWritable()) {
					key.interestOps(key.interestOps() - SelectionKey.OP_WRITE);
					TcpNioConnection<T> connection;
					connection = (TcpNioConnection<T>) key.attachment();
					connection.getConnectionReactor().notify(WRITE_KEY, Fn.event(key));
				}
				if (key.isAcceptable()) {
					try {
						handleAcceptSelection(now);
					}
					catch (Exception e) {
						logger.error("Exception accepting new connection", e);
					}
				}
			}
		}
		catch (CancelledKeyException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Selection key " + key + " cancelled");
			}
		}
		catch (Exception e) {
			logger.error("Exception on selection key " + key, e);
		}
	}

	protected void handleAcceptSelection(long now) throws IOException {
		throw new UnsupportedOperationException("Accepts not supported by this factory");
	}

	protected void traceSelections(int selectionCount) {
		if (host == null) {
			logger.trace("Port " + this.port + " SelectionCount: " + selectionCount);
		} else {
			logger.trace("Host " + this.host + " port " + this.port + " SelectionCount: " + selectionCount);
		}
	}

	/**
	 * @param selector
	 * @param now
	 * @throws IOException
	 */
	protected void doAccept(final Selector selector, ServerSocketChannel server, long now) throws IOException {
		throw new UnsupportedOperationException("Nio server factory must override this method");
	}

	protected void addConnection(TcpConnectionSupport<T> connection) {
		synchronized (this.connections) {
			if (!this.active) {
				connection.close();
				return;
			}
			this.connections.add(connection);
		}
	}

	/**
	 * Cleans up this.connections by removing any closed connections.
	 * @return a list of open connection ids.
	 */
	private List<String> removeClosedConnectionsAndReturnOpenConnectionIds() {
		synchronized (this.connections) {
			List<String> openConnectionIds = new ArrayList<String>();
			Iterator<TcpConnectionSupport<T>> iterator = this.connections.iterator();
			while (iterator.hasNext()) {
				TcpConnection<T> connection = iterator.next();
				if (!connection.isOpen()) {
					iterator.remove();
				}
				else {
					openConnectionIds.add(connection.getConnectionId());
				}
			}
			return openConnectionIds;
		}
	}

	/**
	 * Cleans up this.connections by removing any closed connections.
	 */
	protected void harvestClosedConnections() {
		this.removeClosedConnectionsAndReturnOpenConnectionIds();
	}

	/**
	 * @return the active
	 */
	protected boolean isActive() {
		return active;
	}

	/**
	 * @param active the active to set
	 */
	protected void setActive(boolean active) {
		this.active = active;
	}

	protected void checkActive() throws IOException {
		if (!this.isActive()) {
			throw new IOException(this + " connection factory has not been started");
		}
	}

	protected TcpSocketConfigurer getTcpSocketSupport() {
		return tcpSocketSupport;
	}

	public void setTcpSocketSupport(TcpSocketConfigurer tcpSocketSupport) {
		Assert.notNull(tcpSocketSupport, "TcpSocketSupport must not be null");
		this.tcpSocketSupport = tcpSocketSupport;
	}

	/**
	 * Returns a list of (currently) open {@link TcpConnection} connection ids; allows,
	 * for example, broadcast operations to all open connections.
	 * @return the list of connection ids.
	 */
	public List<String> getOpenConnectionIds() {
		return Collections.unmodifiableList(this.removeClosedConnectionsAndReturnOpenConnectionIds());
	}

	/**
	 * Close a connection with the specified connection id.
	 * @param connectionId the connection id.
	 * @return true if the connection was closed.
	 */
	public boolean closeConnection(String connectionId) {
		Assert.notNull(connectionId, "'connectionId' to close must not be null");
		synchronized(this.connections) {
			boolean closed = false;
			for (TcpConnectionSupport<T> connection : connections) {
				if (connectionId.equals(connection.getConnectionId())) {
					try {
						connection.close();
						closed = true;
						break;
					}
					catch (Exception e) {
						if (logger.isDebugEnabled()) {
							logger.debug("Failed to close connection " + connectionId, e);
						}
						connection.publishConnectionExceptionEvent(e);
					}
				}
			}
			return closed;
		}
	}

}
