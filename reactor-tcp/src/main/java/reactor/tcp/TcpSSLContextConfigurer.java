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
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;

/**
 * Strategy interface for the creation of an {@link SSLContext} object
 * for use with SSL/TLS sockets.
 * @author Gary Russell
 * @since 2.2
 *
 */
public interface TcpSSLContextConfigurer {

	/**
	 * Gets an SSLContext.
	 * @return the SSLContext.
	 * @throws GeneralSecurityException
	 * @throws IOException
	 */
	SSLContext getSSLContext() throws GeneralSecurityException, IOException;

}
