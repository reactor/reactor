package reactor.net.tcp.ssl;

import reactor.function.Supplier;
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
