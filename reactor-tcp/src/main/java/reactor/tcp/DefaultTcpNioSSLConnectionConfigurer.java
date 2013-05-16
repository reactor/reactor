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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import reactor.support.Assert;

/**
 * Implementation of {@link TcpNioConnectionConfigurer} for SSL
 * NIO connections.
 * @author Gary Russell
 * @since 2.2
 *
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
	public TcpNioConnection createNewConnection(SocketChannel socketChannel, boolean server, boolean lookupHost, ConnectionFactorySupport connectionFactory) throws Exception {
		SSLEngine sslEngine = this.sslContext.createSSLEngine();
		TcpNioSSLConnection tcpNioSSLConnection = new TcpNioSSLConnection(socketChannel, server, lookupHost, connectionFactory, sslEngine);
		tcpNioSSLConnection.init();
		return tcpNioSSLConnection;
	}
}
