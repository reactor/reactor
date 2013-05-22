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

import reactor.fn.Supplier;
import reactor.tcp.codec.Codec;
import reactor.util.Assert;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;

/**
 * Implementation of {@link TcpNioConnectionConfigurer} for SSL NIO connections.
 *
 * @author Gary Russell
 */
public class DefaultTcpNioSSLConnectionConfigurer implements TcpNioConnectionConfigurer {

	private volatile SSLContext sslContext;

	public DefaultTcpNioSSLConnectionConfigurer(TcpSSLContextConfigurer sslContextSupport) {
		Assert.notNull(sslContextSupport, "TcpSSLContextSupport must not be null");
		try {
			this.sslContext = sslContextSupport.getSSLContext();
			Assert.notNull(this.sslContext, "SSLContex must not be null");
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		} catch (GeneralSecurityException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Creates a {@link TcpNioSSLConnection}.
	 */
	public <T> TcpNioConnection<T> createNewConnection(SocketChannel socketChannel, boolean server, boolean lookupHost, ConnectionFactorySupport<T> connectionFactory, Supplier<Codec<T>> codecSupplier) throws Exception {
		SSLEngine sslEngine = this.sslContext.createSSLEngine();
		TcpNioSSLConnection<T> tcpNioSSLConnection = new TcpNioSSLConnection<T>(socketChannel, server, lookupHost, connectionFactory, sslEngine, codecSupplier.get());
		tcpNioSSLConnection.init();
		return tcpNioSSLConnection;
	}
}
