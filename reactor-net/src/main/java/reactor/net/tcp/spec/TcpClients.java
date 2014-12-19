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

package reactor.net.tcp.spec;

import reactor.Environment;
import reactor.net.tcp.TcpClient;

import javax.annotation.Nonnull;

/**
 * Helper class to make creating {@link reactor.net.tcp.TcpClient} instances more succinct.
 *
 * @author Jon Brisbin
 */
public class TcpClients {

	/**
	 * Create a {@link TcpClientSpec} for further configuration using the given {@link
	 * reactor.Environment} and {@code clientImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.Environment} to use
	 * @param clientImpl
	 * 		the {@link reactor.net.tcp.TcpClient} implementation to use
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a new {@link TcpClientSpec}
	 */
	public static <IN, OUT> TcpClientSpec<IN, OUT> create(Environment env,
	                                                      @Nonnull Class<? extends TcpClient> clientImpl) {
		return new TcpClientSpec<IN, OUT>(clientImpl).env(env);
	}

	/**
	 * Create a {@link TcpClientSpec} for further configuration using the given {@link
	 * reactor.Environment}, {@link reactor.core.Dispatcher} type, and {@code clientImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.Environment} to use
	 * @param dispatcher
	 * 		the type of {@link reactor.core.Dispatcher} to use
	 * @param clientImpl
	 * 		the {@link reactor.net.tcp.TcpClient} implementation to use
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a new {@link TcpClientSpec}
	 */
	public static <IN, OUT> TcpClientSpec<IN, OUT> create(Environment env,
	                                                      String dispatcher,
	                                                      @Nonnull Class<? extends TcpClient> clientImpl) {
		return new TcpClientSpec<IN, OUT>(clientImpl).env(env).dispatcher(dispatcher);
	}

}
