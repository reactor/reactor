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

package reactor.net;

import reactor.tuple.Tuple2;

import java.net.InetSocketAddress;

/**
 * Implementations of this interface will be instantiated by a {@link reactor.function.Supplier} to provide information
 * to the {@link reactor.net.tcp.TcpClient} whether or not to attempt to reconnect a broken connection.
 * <p/>
 * The {@link #reconnect(java.net.InetSocketAddress, int)} method will be invoked, passing the currently-connected
 * address and the number of times a reconnection has been attempted on this connection. If the client is to reconnect
 * to a different host, then provide that different address in the return value. If you don't want to try and reconnect
 * at all, simply return {@code null}.
 *
 * @author Jon Brisbin
 */
public interface Reconnect {

	/**
	 * Provide an {@link InetSocketAddress} to which a reconnection attempt should be made.
	 *
	 * @param currentAddress the address to which the client is currently connected
	 * @return a possibly different {@link InetSocketAddress} to which a reconnection attempt should be made and a {@code
	 *         Long} denoting the time to delay a reconnection attempt
	 */
	Tuple2<InetSocketAddress, Long> reconnect(InetSocketAddress currentAddress, int attempt);

}
