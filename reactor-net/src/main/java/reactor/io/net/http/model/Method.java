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
package reactor.io.net.http.model;

import reactor.core.support.Assert;

/**
 * A Model representation of supported HTTP Methods
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class Method {

	public static final Method GET     = new Method("GET");
	public static final Method POST    = new Method("POST");
	public static final Method PUT     = new Method("PUT");
	public static final Method PATCH   = new Method("PATCH");
	public static final Method DELETE  = new Method("DELETE");
	public static final Method OPTIONS = new Method("OPTIONS");
	public static final Method HEAD    = new Method("HEAD");
	public static final Method TRACE   = new Method("TRACE");
	public static final Method CONNECT = new Method("CONNECT");
	public static final Method BEFORE  = new Method("BEFORE");
	public static final Method AFTER   = new Method("AFTER");
	public static final Method WS      = new Method("WS");

	private final String name;

	public Method(String name) {
		Assert.hasText(name);
		this.name = name;
	}

	public String getName() {
		return name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Method method = (Method) o;

		if (!name.equals(method.name)) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}
}