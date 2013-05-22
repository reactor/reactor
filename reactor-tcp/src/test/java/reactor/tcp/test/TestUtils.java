/*
 * Copyright 2002-2012 the original author or authors.
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
package reactor.tcp.test;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.net.ServerSocketFactory;

import reactor.support.Assert;
import reactor.tcp.AbstractServerConnectionFactory;
import reactor.tcp.ConnectionFactorySupport;
import reactor.tcp.test.TimeoutUtils.TimeoutCallback;

/**
 *
 * Contains several socket-specific utility methods. For example, you may have
 * test cases that require an open port. Rather than hard-coding the relevant port,
 * it will be better to use methods from this utility class to automatically select
 * an open port, thereby improving the portability of your test-cases across
 * systems.
 *
 * @author Gunnar Hillert
 * @author Andy Wilkinson
 *
 */
public final class TestUtils {

	public static final int DEFAULT_PORT_RANGE_MIN = 10000;
	public static final int DEFAULT_PORT_RANGE_MAX = 60000;

	/**
	 * The constructor is intentionally public. In several test cases you may have
	 * the need to use the methods of this class multiple times from within your
	 * Spring Application Context XML file using SpEL. Of course you can do:
	 *
	 * <pre>
	 * {@code
	 * ...port="#{T(org.springframework.integration.test.util.SocketUtils).findAvailableServerSocket(12000)}"
	 * }
	 * </pre>
	 *
	 * But unfortunately, you would need to repeat the package for each usage.
	 * This will be acceptable for single use, but if you need to invoke the
	 * methods numerous time, you may instead want to do this:
	 *
	 * <pre>
	 * {@code
	 * <bean id="tcpIpUtils" class="org.springframework.integration.test.util.SocketUtils" />
	 *
	 * ...port="#{tcpIpUtils.findAvailableServerSocket(12000)}"
	 * }
	 * </pre>
	 *
	 */
	public TestUtils() { }

	/**
	 * Determines a free available server socket (port) using the 'seed' value as
	 * the starting port. The utility methods will probe for 200 sockets but will
	 * return as soon an open port is found.
	 *
	 * @param seed The starting port, which must not be negative.
	 * @return An available port number
	 *
	 * @throws IllegalStateException when no open port was found.
	 */
	public static int findAvailableServerSocket(int seed) {
		final List<Integer> openPorts = findAvailableServerSockets(seed, 1);
		return openPorts.get(0);
	}

	/**
	 * Determines a free available server socket (port) using the 'seed' value as
	 * the starting port. The utility methods will probe for 200 sockets but will
	 * return as soon an open port is found.
	 *
	 * @param seed The starting port, which must not be negative.
	 * @param numberOfRequestedPorts How many open ports shall be retrieved?
	 * @return A list containing the requested number of open ports
	 *
	 * @throws IllegalStateException when no open port was found.
	 */
	public static List<Integer> findAvailableServerSockets(int seed, int numberOfRequestedPorts) {

		Assert.isTrue(seed >= 0, "'seed' must not be negative");
		Assert.isTrue(numberOfRequestedPorts > 0, "'numberOfRequestedPorts' must not be negative");

		final List<Integer> openPorts = new ArrayList<Integer>(numberOfRequestedPorts);

		for (int i = seed; i < seed+200; i++) {
			try {
				ServerSocket sock = ServerSocketFactory.getDefault().createServerSocket(i);
				sock.close();
				openPorts.add(i);

				if (openPorts.size() == numberOfRequestedPorts) {
					return openPorts;
				}

			} catch (Exception e) { }
		}

		throw new IllegalStateException(String.format("Cannot find a free server socket (%s requested)", numberOfRequestedPorts));
	}

	/**
	 * Determines a free available server socket (port) using an automatically
	 * chosen start seed port.
	 *
	 * @return An available port number
	 *
	 * @throws IllegalStateException when no open port was found.
	 */
	public static int findAvailableServerSocket() {
		int seed = getRandomSeedPort();
		return findAvailableServerSocket(seed);
	}

	/**
	 * Determines a random seed port number within the port range
	 * {@value #DEFAULT_PORT_RANGE_MIN} and {@value #DEFAULT_PORT_RANGE_MAX}.
	 *
	 * @return A number with the the specified range
	 */
	public static int getRandomSeedPort() {

		final Random random = new Random();

		int randomNumber = random.nextInt(DEFAULT_PORT_RANGE_MAX - DEFAULT_PORT_RANGE_MIN + 1) + DEFAULT_PORT_RANGE_MIN;

		return randomNumber;
	}

	/**
	 * Wait for a server connection factory to actually start listening before
	 * starting a test. Waits for up to 30 seconds
	 *
	 * @param serverConnectionFactory The server connection factory.
	 *
	 * @throws IllegalStateException if the server is not listening after 30 seconds
	 * @throws InterruptedException if interrupted while waiting
	 */
	public static void waitListening(final AbstractServerConnectionFactory<?> serverConnectionFactory) throws IllegalStateException, InterruptedException {
		TimeoutUtils.doWithTimeout(30000,  new TimeoutCallback() {
			@Override
			public boolean isComplete() {
				return serverConnectionFactory.isListening();
			}
		});
	}

	/**
	 * Wait for a server connection factory to stop listening.
	 * Waits for up to 30 seconds.
	 *
	 * @param serverConnectionFactory The server connection factory.
	 *
	 * @throws IllegalStateException if the server has not stopped listening after 30 seconds
	 * @throws InterruptedException if interrupted while waiting
	 */
	public static void waitStopListening(final AbstractServerConnectionFactory<?> serverConnectionFactory) throws IllegalStateException, InterruptedException {
		TimeoutUtils.doWithTimeout(30000,  new TimeoutCallback() {
			@Override
			public boolean isComplete() {
				return serverConnectionFactory.isListening();
			}
		});
	}

	/**
	 * Wait for up to 10 seconds for the connection factory to have the specified number
	 * of connections.
	 *
	 * @param factory The factory.
	 * @param requiredConnections The required number of connections.
	 *
	 * @throws IllegalStateException if the count does not match.
	 * @throws InterruptedException if interrupted while waiting
	 */
	public static void waitUntilFactoryHasThisNumberOfConnections(final ConnectionFactorySupport<?> factory, final int requiredConnections)
			throws Exception {
		TimeoutUtils.doWithTimeout(10000,  new TimeoutCallback() {

			@Override
			public boolean isComplete() {
				return factory.getOpenConnectionIds().size() >= requiredConnections;
			}

		});
	}

}
