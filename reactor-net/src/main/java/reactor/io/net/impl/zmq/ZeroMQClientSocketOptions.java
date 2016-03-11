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

package reactor.io.netty.impl.zmq;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.io.netty.config.ClientSocketOptions;

/**
 * {@link reactor.io.netty.config.ClientSocketOptions} that include ZeroMQ-specific configuration options.
 *
 * @author Jon Brisbin
 */
public class ZeroMQClientSocketOptions extends ClientSocketOptions {

	private ZContext context;
	private int socketType = ZMQ.ROUTER;
	private Consumer<ZMQ.Socket> socketConfigurer;
	private String connectAddresses;

	/**
	 * Get the {@link org.zeromq.ZContext} to use for IO.
	 *
	 * @return the {@link org.zeromq.ZContext} to use
	 */
	public ZContext context() {
		return context;
	}

	/**
	 * Set the {@link org.zeromq.ZContext} to use for IO.
	 *
	 * @param context
	 * 		the {@link org.zeromq.ZContext} to use
	 *
	 * @return {@literal this}
	 */
	public ZeroMQClientSocketOptions context(ZContext context) {
		Assert.notNull(context, "ZeroMQ Context cannot be null");
		this.context = context;
		return this;
	}

	/**
	 * The type of the ZMQ socket to create.
	 *
	 * @return the ZMQ socket type
	 */
	public int socketType() {
		return socketType;
	}

	/**
	 * Set the type of ZMQ socket to create.
	 *
	 * @param socketType
	 * 		the ZMQ socket type
	 *
	 * @return {@literal this}
	 */
	public ZeroMQClientSocketOptions socketType(int socketType) {
		this.socketType = socketType;
		return this;
	}

	/**
	 * The {@link reactor.fn.Consumer} responsible for configuring the underlying ZeroMQ socket.
	 *
	 * @return the ZMQ.Socket configurer
	 */
	public Consumer<ZMQ.Socket> socketConfigurer() {
		return socketConfigurer;
	}

	/**
	 * Set the {@link reactor.fn.Consumer} responsible for configure the underlying ZeroMQ socket.
	 *
	 * @param socketConfigurer
	 * 		the ZMQ.Socket configurer
	 *
	 * @return {@literal this}
	 */
	public ZeroMQClientSocketOptions socketConfigurer(Consumer<ZMQ.Socket> socketConfigurer) {
		this.socketConfigurer = socketConfigurer;
		return this;
	}

	public String connectAddresses() {
		return connectAddresses;
	}

	public ZeroMQClientSocketOptions connectAddresses(String connectAddresses) {
		this.connectAddresses = connectAddresses;
		return this;
	}

}
