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

package reactor.io.net.zmq;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.io.net.config.ServerSocketOptions;

/**
 * {@link reactor.io.net.config.ServerSocketOptions} that include ZeroMQ-specific configuration options.
 *
 * @author Jon Brisbin
 */
public class ZeroMQServerSocketOptions extends ServerSocketOptions {

	private ZContext context;
	private int socketType = ZMQ.ROUTER;
	private Consumer<ZMQ.Socket> socketConfigurer;
	private String               listenAddresses;

	/**
	 * Get the {@link org.zeromq.ZMQ.Context} to use for IO.
	 *
	 * @return the {@link org.zeromq.ZMQ.Context} to use
	 */
	public ZContext context() {
		return context;
	}

	/**
	 * Set the {@link org.zeromq.ZMQ.Context} to use for IO.
	 *
	 * @param context
	 * 		the {@link org.zeromq.ZMQ.Context} to use
	 *
	 * @return {@literal this}
	 */
	public ZeroMQServerSocketOptions context(ZContext context) {
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
	public ZeroMQServerSocketOptions socketType(int socketType) {
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
	public ZeroMQServerSocketOptions socketConfigurer(Consumer<ZMQ.Socket> socketConfigurer) {
		this.socketConfigurer = socketConfigurer;
		return this;
	}


	public String listenAddresses() {
		return listenAddresses;
	}

	public ZeroMQServerSocketOptions listenAddresses(String listenAddresses) {
		this.listenAddresses = listenAddresses;
		return this;
	}

}
