/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.net.config;

import reactor.io.buffer.Buffer;

/**
 * Encapsulates common socket options.
 *
 * @param <SO> A CommonSocketOptions subclass
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings("unchecked")
public abstract class CommonSocketOptions<SO extends CommonSocketOptions<? super SO>> {

	private int     timeout    = 30000;
	private boolean keepAlive  = true;
	private int     linger     = 30000;
	private boolean tcpNoDelay = true;
	private int     rcvbuf     = Buffer.SMALL_BUFFER_SIZE;
	private int     sndbuf     = Buffer.SMALL_BUFFER_SIZE;
	private long    prefetch   = -1l;

	/**
	 * Gets the {@code SO_TIMEOUT} value
	 *
	 * @return the timeout value
	 */
	public int timeout() {
		return timeout;
	}

	/**
	 * Set the {@code SO_TIMEOUT} value.
	 *
	 * @param timeout The {@code SO_TIMEOUT} value.
	 * @return {@code this}
	 */
	public SO timeout(int timeout) {
		this.timeout = timeout;
		return (SO) this;
	}

	/**
	 * Gets the {@code prefetch} maximum in-flight value
	 *
	 * @return the prefetch value, {@code -1l} if undefined
	 */
	public long prefetch() {
		return prefetch;
	}

	/**
	 * Set the Consuming capacity along with eventual flushing strategy each given prefetch iteration.
	 * Long.MAX will instruct the channels to be unbounded (e.g. limited by the dispatcher capacity if any or a slow consumer).
	 * When unbounded the system will take a maximum of data off the channel incoming connection.
	 * Setting a value of 10 will however pause the channel after 10 successful reads until the next request from the consumer.
	 *
	 * @param prefetch The {@code prefetch} in-flight data over this channel ({@code Long.MAX_VALUE} for unbounded).
	 * @return {@code this}
	 */
	public SO prefetch(long prefetch) {
		this.prefetch = prefetch;
		return (SO) this;
	}

	/**
	 * Returns a boolean indicating whether or not {@code SO_KEEPALIVE} is enabled
	 *
	 * @return {@code true} if keep alive is enabled, {@code false} otherwise
	 */
	public boolean keepAlive() {
		return keepAlive;
	}

	/**
	 * Enables or disables {@code SO_KEEPALIVE}.
	 *
	 * @param keepAlive {@code true} to enable keepalive, {@code false} to disable keepalive
	 * @return {@code this}
	 */
	public SO keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
		return (SO) this;
	}

	/**
	 * Returns the configuration of {@code SO_LINGER}.
	 *
	 * @return the value of {@code SO_LINGER} in seconds
	 */
	public int linger() {
		return linger;
	}

	/**
	 * Configures {@code SO_LINGER}
	 *
	 * @param linger The linger period in seconds
	 * @return {@code this}
	 */
	public SO linger(int linger) {
		this.linger = linger;
		return (SO) this;
	}

	/**
	 * Returns a boolean indicating whether or not {@code TCP_NODELAY} is enabled
	 *
	 * @return {@code true} if {@code TCP_NODELAY} is enabled, {@code false} if it is not
	 */
	public boolean tcpNoDelay() {
		return tcpNoDelay;
	}

	/**
	 * Enables or disables {@code TCP_NODELAY}
	 *
	 * @param tcpNoDelay {@code true} to enable {@code TCP_NODELAY}, {@code false} to disable it
	 * @return {@code this}
	 */
	public SO tcpNoDelay(boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
		return (SO) this;
	}

	/**
	 * Gets the configured {@code SO_RCVBUF} (receive buffer) size
	 *
	 * @return The configured receive buffer size
	 */
	public int rcvbuf() {
		return rcvbuf;
	}

	/**
	 * Sets the {@code SO_RCVBUF} (receive buffer) size
	 *
	 * @param rcvbuf The size of the receive buffer
	 * @return {@code this}
	 */
	public SO rcvbuf(int rcvbuf) {
		this.rcvbuf = rcvbuf;
		return (SO) this;
	}

	/**
	 * Gets the configured {@code SO_SNDBUF} (send buffer) size
	 *
	 * @return The configured send buffer size
	 */
	public int sndbuf() {
		return sndbuf;
	}

	/**
	 * Sets the {@code SO_SNDBUF} (send buffer) size
	 *
	 * @param sndbuf The size of the send buffer
	 * @return {@code this}
	 */
	public SO sndbuf(int sndbuf) {
		this.sndbuf = sndbuf;
		return (SO) this;
	}

}
