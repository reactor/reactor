/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.io.netty.http.model;

/**
 * A Model representation of supported HTTP Protocols
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public enum Protocol {

	HTTP_1_0("http/1.0"), HTTP_1_1("http/1.1"), HTTP_2_0("h2-15");

	private final String protocol;

	Protocol(String protocol) {
		this.protocol = protocol;
	}

	public static Protocol get(String protocol) {
		if (protocol.equals(HTTP_1_0.protocol)) return HTTP_1_0;
		if (protocol.equals(HTTP_1_1.protocol)) return HTTP_1_1;
		if (protocol.equals(HTTP_2_0.protocol)) return HTTP_2_0;
		throw new IllegalArgumentException("Unexpected protocol: " + protocol);
	}

	@Override public String toString() {
		return this.protocol;
	}
}
