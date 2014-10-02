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

package reactor.core.support;

import reactor.core.Environment;
import reactor.function.Supplier;

/**
 * A default implementation of a {@link reactor.function.Supplier} that provides a singleton {@link reactor.core
 * .Environment} which is created in the constructor.
 *
 * @author Jon Brisbin
 */
public class DefaultEnvironmentSupplier implements Supplier<Environment> {

	private final Environment env;

	public DefaultEnvironmentSupplier() {
		this.env = new Environment();
	}

	@Override
	public Environment get() {
		return env;
	}

}
