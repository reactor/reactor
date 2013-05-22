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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import reactor.util.Assert;

/**
 * Default implementation of {@link TcpSSLContextConfigurer}; uses a
 * 'TLS' (by default) {@link SSLContext}, initialized with 'JKS'
 * keystores, managed by 'SunX509' Key and Trust managers.
 * @author Gary Russell
 *
 */
public class DefaultTcpSSLContextConfigurer implements TcpSSLContextConfigurer {

	private final File keyStore;

	private final File trustStore;

	private final char[] keyStorePassword;

	private final char[] trustStorePassword;

	private volatile String protocol = "TLS";

	/**
	 * Prepares for the creation of an SSLContext using the supplied
	 * key/trust stores and passwords.
	 * @param keyStore The SSL keystore file
	 * @param trustStore The SSL truststore file
	 * @param keyStorePassword The password for the keyStore.
	 * @param trustStorePassword The password for the trustStore.
	 */
	public DefaultTcpSSLContextConfigurer(File keyStore, File trustStore,
			String keyStorePassword, String trustStorePassword) {
		Assert.notNull(keyStore, "keyStore cannot be null");
		Assert.notNull(trustStore, "trustStore cannot be null");
		Assert.notNull(keyStorePassword, "keyStorePassword cannot be null");
		Assert.notNull(trustStorePassword, "trustStorePassword cannot be null");
		this.keyStore = keyStore;
		this.trustStore = trustStore;
		this.keyStorePassword = keyStorePassword.toCharArray();
		this.trustStorePassword = trustStorePassword.toCharArray();
	}

	@Override
	public SSLContext getSSLContext() throws GeneralSecurityException, IOException  {
		KeyStore ks = KeyStore.getInstance("JKS");
		KeyStore ts = KeyStore.getInstance("JKS");

		ks.load(new FileInputStream(keyStore), keyStorePassword);
		ts.load(new FileInputStream(trustStore), trustStorePassword);

		KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		kmf.init(ks, keyStorePassword);

		TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
		tmf.init(ts);

		SSLContext sslContext = SSLContext.getInstance(protocol);

		sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

		return sslContext;

	}

	/**
	 * The protocol used in {@link SSLContext#getInstance(String)}; default "TLS".
	 * @param protocol The protocol.
	 */
	public void setProtocol(String protocol) {
		Assert.notNull(protocol, "protocol must not be null");
		this.protocol = protocol;
	}

}
