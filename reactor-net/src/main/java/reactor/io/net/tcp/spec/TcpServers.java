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

package reactor.io.net.tcp.spec;

import reactor.Environment;
import reactor.io.net.tcp.TcpServer;

import javax.annotation.Nonnull;

/**
 * Helper class to make creating {@link reactor.io.net.tcp.TcpServer} instances more succinct.
 *
 * @author Jon Brisbin
 */
public abstract class TcpServers {

	protected TcpServers() {
	}

	/**
	 * Create a {@link TcpServerSpec} for further configuration using the given {@link
	 * reactor.Environment} and {@code serverImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.Environment} to use
	 * @param serverImpl
	 * 		the implementation of {@link reactor.io.net.tcp.TcpServer}
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a {@link TcpServerSpec} to be further configured
	 */
	public static <IN, OUT> TcpServerSpec<IN, OUT> create(Environment env,
	                                                      @Nonnull Class<? extends TcpServer> serverImpl) {
		return new TcpServerSpec<IN, OUT>(serverImpl).env(env);
	}

	/**
	 * Create a {@link TcpServerSpec} for further configuration using the given {@link
	 * reactor.Environment} and {@code serverImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.Environment} to use
	 * @param dispatcher
	 * 		the type of dispatcher to use
	 * @param serverImpl
	 * 		the implementation of {@link reactor.io.net.tcp.TcpServer}
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a {@link TcpServerSpec} to be further configured
	 */
	public static <IN, OUT> TcpServerSpec<IN, OUT> create(Environment env,
	                                                      String dispatcher,
	                                                      @Nonnull Class<? extends TcpServer> serverImpl) {
		return new TcpServerSpec<IN, OUT>(serverImpl).env(env).dispatcher(dispatcher);
	}

}
