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

import java.nio.channels.SocketChannel;


/**
 * Used by NIO connection factories to instantiate a {@link TcpNioConnection} object.
 * Implementations for SSL and non-SSL {@link TcpNioConnection}s are provided.
 * @author Gary Russell
 *
 */
public interface TcpNioConnectionConfigurer {

	/**
	 * Create a new {@link TcpNioConnection} object wrapping the {@link SocketChannel}
	 * @param socketChannel the SocketChannel.
	 * @param server true if this connection is a server connection.
	 * @param lookupHost true if hostname lookup should be performed, otherwise the connection will
	 * be identified using the ip address.
	 * @param connectionFactory the connection factory creating this connection; used
	 * during event publishing, may be null, in which case "unknown" will be used.
	 * @return the TcpNioConnection
	 * @throws Exception
	 */
	TcpNioConnection createNewConnection(SocketChannel socketChannel,
			boolean server, boolean lookupHost, ConnectionFactorySupport connectionFactory) throws Exception;
}
