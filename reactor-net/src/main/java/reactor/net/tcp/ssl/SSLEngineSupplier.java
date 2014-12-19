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

package reactor.net.tcp.ssl;

import reactor.fn.Supplier;
import reactor.net.config.SslOptions;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * @author Jon Brisbin
 */
public class SSLEngineSupplier implements Supplier<SSLEngine> {

	private final SSLEngine ssl;

	public SSLEngineSupplier(SslOptions sslOpts, boolean client) throws Exception {
		if (null == sslOpts || null == sslOpts.keystoreFile()) {
			ssl = SSLContext.getDefault().createSSLEngine();
			ssl.setUseClientMode(client);
			return;
		}

		KeyStore ks = KeyStore.getInstance("JKS");
		FileInputStream ksin = new FileInputStream(sslOpts.keystoreFile());
		ks.load(ksin, sslOpts.keystorePasswd().toCharArray());

		SSLContext ctx = SSLContext.getInstance(sslOpts.sslProtocol());
		KeyManager[] keyManagers;
		TrustManager[] trustManagers;
		if (null != sslOpts.trustManagers()) {
			trustManagers = sslOpts.trustManagers().get();
		} else {
			TrustManagerFactory tmf = TrustManagerFactory.getInstance(sslOpts.trustManagerFactoryAlgorithm());
			tmf.init(ks);
			trustManagers = tmf.getTrustManagers();
		}

		KeyManagerFactory kmf = KeyManagerFactory.getInstance(sslOpts.keyManagerFactoryAlgorithm());
		kmf.init(ks, (null != sslOpts.keyManagerPasswd() ? sslOpts.keyManagerPasswd() : sslOpts.keystorePasswd()).toCharArray());
		keyManagers = kmf.getKeyManagers();

		ctx.init(keyManagers, trustManagers, null);

		ssl = ctx.createSSLEngine();
		ssl.setUseClientMode(client);
	}

	@Override
	public SSLEngine get() {
		return ssl;
	}

}
