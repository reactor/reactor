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

package reactor.net.udp.spec;

import reactor.Environment;
import reactor.net.udp.DatagramServer;

/**
 * Helper class to make creating {@link reactor.net.udp.DatagramServer} instances in code more succinct.
 *
 * @author Jon Brisbin
 */
public class DatagramServers {

	/**
	 * Create a {@link DatagramServerSpec} for further configuration using the given {@link
	 * reactor.Environment} and {@code serverImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.Environment} to use
	 * @param serverImpl
	 * 		the implementation of {@link reactor.net.udp.DatagramServer} to use
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a new {@link DatagramServerSpec}
	 */
	public static <IN, OUT> DatagramServerSpec<IN, OUT> create(Environment env,
	                                                           Class<? extends DatagramServer> serverImpl) {
		return new DatagramServerSpec<IN, OUT>(serverImpl).env(env);
	}

	/**
	 * Create a {@link DatagramServerSpec} for further configuration using the given {@link
	 * reactor.Environment}, {@link reactor.core.Dispatcher} type, and {@code serverImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.Environment} to use
	 * @param dispatcher
	 * 		the type of {@link reactor.core.Dispatcher} to use
	 * @param serverImpl
	 * 		the implementation of {@link reactor.net.udp.DatagramServer} to use
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a new {@link DatagramServerSpec}
	 */
	public static <IN, OUT> DatagramServerSpec<IN, OUT> create(Environment env,
	                                                           String dispatcher,
	                                                           Class<? extends DatagramServer> serverImpl) {
		return new DatagramServerSpec<IN, OUT>(serverImpl).env(env).dispatcher(dispatcher);
	}

}
