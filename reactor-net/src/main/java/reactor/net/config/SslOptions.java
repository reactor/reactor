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

package reactor.net.config;

import reactor.core.support.Assert;
import reactor.fn.Supplier;

import javax.net.ssl.TrustManager;
import java.io.File;

/**
 * Helper class encapsulating common SSL configuration options.
 *
 * @author Jon Brisbin
 */
public class SslOptions {

	private File   keystoreFile;
	private String keystorePasswd;
	private String keyManagerPasswd;
	private String keyManagerFactoryAlgorithm = "SunX509";
	private Supplier<TrustManager[]> trustManagers;
	private String                   trustManagerPasswd;
	private String trustManagerFactoryAlgorithm = "SunX509";
	private String sslProtocol                  = "TLS";

	public String keystoreFile() {
		return (null != keystoreFile ? keystoreFile.getPath() : null);
	}

	public SslOptions keystoreFile(String keystoreFile) {
		this.keystoreFile = new File(keystoreFile);
		Assert.isTrue(this.keystoreFile.exists(), "No keystore file found at path " + this.keystoreFile.getAbsolutePath());
		return this;
	}

	public String keystorePasswd() {
		return keystorePasswd;
	}

	public SslOptions keystorePasswd(String keystorePasswd) {
		this.keystorePasswd = keystorePasswd;
		return this;
	}

	public String keyManagerPasswd() {
		return keyManagerPasswd;
	}

	public SslOptions keyManagerPasswd(String keyManagerPasswd) {
		this.keyManagerPasswd = keyManagerPasswd;
		return this;
	}

	public String keyManagerFactoryAlgorithm() {
		return keyManagerFactoryAlgorithm;
	}

	public SslOptions keyManagerFactoryAlgorithm(String keyManagerFactoryAlgorithm) {
		Assert.notNull(keyManagerFactoryAlgorithm, "KeyManagerFactory algorithm cannot be null");
		this.keyManagerFactoryAlgorithm = keyManagerFactoryAlgorithm;
		return this;
	}

	public String trustManagerPasswd() {
		return trustManagerPasswd;
	}

	public SslOptions trustManagerPasswd(String trustManagerPasswd) {
		this.trustManagerPasswd = trustManagerPasswd;
		return this;
	}

	public Supplier<TrustManager[]> trustManagers() {
		return trustManagers;
	}

	public SslOptions trustManagers(Supplier<TrustManager[]> trustManagers) {
		this.trustManagers = trustManagers;
		return this;
	}

	public String trustManagerFactoryAlgorithm() {
		return trustManagerFactoryAlgorithm;
	}

	public SslOptions trustManagerFactoryAlgorithm(String trustManagerFactoryAlgorithm) {
		Assert.notNull(trustManagerFactoryAlgorithm, "TrustManagerFactory algorithm cannot be null");
		this.trustManagerFactoryAlgorithm = trustManagerFactoryAlgorithm;
		return this;
	}

	public String sslProtocol() {
		return sslProtocol;
	}

	public SslOptions sslProtocol(String sslProtocol) {
		Assert.notNull(sslProtocol, "SSL protocol cannot be null");
		this.sslProtocol = sslProtocol;
		return this;
	}

}
