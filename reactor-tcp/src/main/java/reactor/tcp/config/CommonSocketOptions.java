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

package reactor.tcp.config;

import reactor.io.Buffer;

/**
 * Encapsulates common socket options.
 *
 * @author Jon Brisbin
 */
public abstract class CommonSocketOptions<SO extends CommonSocketOptions<? super SO>> {

	private int     timeout    = 30000;
	private boolean keepAlive  = true;
	private int     linger     = 30000;
	private boolean tcpNoDelay = true;
	private int     rcvbuf     = Buffer.SMALL_BUFFER_SIZE;
	private int     sndbuf     = Buffer.SMALL_BUFFER_SIZE;

	public int timeout() {
		return timeout;
	}

	/**
	 * Set the {@code SO_TIMEOUT} value.
	 *
	 * @param timeout The {@code SO_TIMEOUT} value.
	 * @return
	 */
	public SO timeout(int timeout) {
		this.timeout = timeout;
		return (SO) this;
	}

	public boolean keepAlive() {
		return keepAlive;
	}

	public SO keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
		return (SO) this;
	}

	public int linger() {
		return linger;
	}

	public SO linger(int linger) {
		this.linger = linger;
		return (SO) this;
	}

	public boolean tcpNoDelay() {
		return tcpNoDelay;
	}

	public SO tcpNoDelay(boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
		return (SO) this;
	}

	public int rcvbuf() {
		return rcvbuf;
	}

	public SO rcvbuf(int rcvbuf) {
		this.rcvbuf = rcvbuf;
		return (SO) this;
	}

	public int sndbuf() {
		return sndbuf;
	}

	public SO sndbuf(int sndbuf) {
		this.sndbuf = sndbuf;
		return (SO) this;
	}

}
