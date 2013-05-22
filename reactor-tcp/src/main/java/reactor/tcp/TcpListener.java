/*
 * Copyright 2001-2013 the original author or authors.
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

/**
 * Classes that implement this interface may register with a
 * connection factory to receive messages retrieved from a
 * {@link TcpConnection}
 *
 * @author Gary Russell
 * @author Andy Wilkinson
 *
 */
public interface TcpListener<T> {

	/**
	 * Called when inbound data has been decoded into an instance of {@code T}.
	 *
	 * @param T          the decoded data
	 * @param connection the TCP connection that the data was received on
	 */
	void onDecode(T decoded, TcpConnection<T> connection);

}