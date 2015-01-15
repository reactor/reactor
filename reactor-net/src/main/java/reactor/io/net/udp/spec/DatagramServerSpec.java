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

package reactor.io.net.udp.spec;

import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.io.codec.Codec;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.spec.NetServerSpec;
import reactor.io.net.udp.DatagramServer;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Collection;

/**
 * @author Jon Brisbin
 */
public class DatagramServerSpec<IN, OUT>
		extends NetServerSpec<IN, OUT, DatagramServerSpec<IN, OUT>, DatagramServer<IN, OUT>> {

	protected final Constructor<? extends DatagramServer> serverImplCtor;

	private NetworkInterface multicastInterface;

	public DatagramServerSpec(Class<? extends DatagramServer> serverImpl) {
		Assert.notNull(serverImpl, "NetServer implementation class cannot be null.");
		try {
			this.serverImplCtor = serverImpl.getDeclaredConstructor(
					Environment.class,
					Dispatcher.class,
					InetSocketAddress.class,
					NetworkInterface.class,
					ServerSocketOptions.class,
					Codec.class,
					Collection.class
			);
			this.serverImplCtor.setAccessible(true);
		} catch(NoSuchMethodException e) {
			throw new IllegalArgumentException(
					"No public constructor found that matches the signature of the one found in the DatagramServer class.");
		}
	}

	/**
	 * Set the interface to use for multicast.
	 *
	 * @param iface
	 * 		the {@link java.net.NetworkInterface} to use for multicast.
	 *
	 * @return {@literal this}
	 */
	public DatagramServerSpec<IN, OUT> multicastInterface(NetworkInterface iface) {
		this.multicastInterface = iface;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected DatagramServer<IN, OUT> configure(Dispatcher dispatcher, Environment environment) {
		try {
			return serverImplCtor.newInstance(
					environment,
					dispatcher,
					listenAddress,
					multicastInterface,
					options,
					codec,
					channelConsumers
			);
		} catch(Throwable t) {
			throw new IllegalStateException(t);
		}
	}

}
