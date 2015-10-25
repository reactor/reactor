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
package reactor.aeron.processor;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * Helper class for creating Aeron subscriptions and publications and
 * publishing messages.
 *
 * @author Anatoly Kadyshev
 */
public class AeronHelper {

	private final boolean launchEmbeddedMediaDriver;

	private final long publicationLingerTimeoutMillis;

	private Aeron aeron;

	/**
	 * How long to try to publish into Aeron before giving up.
	 * @see Context#publicationLingerTimeoutMillis
	 */
	private final long publicationTimeoutNs;

	public AeronHelper(Aeron aeron, boolean launchEmbeddedMediaDriver, long publicationTimeoutMillis,
				long publicationLingerTimeoutMillis) {
		this.launchEmbeddedMediaDriver = launchEmbeddedMediaDriver;
		this.publicationLingerTimeoutMillis = publicationLingerTimeoutMillis;
		this.aeron = aeron;
		this.publicationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(publicationTimeoutMillis);
	}

	static BackoffIdleStrategy newBackoffIdleStrategy() {
		return new BackoffIdleStrategy(
				100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
	}

	static void putUUID(MutableDirectBuffer buffer, int offset,
						long mostSignificantBits, long leastSignificantBits) {
		buffer.putLong(offset, mostSignificantBits);
		buffer.putLong(offset + 8, leastSignificantBits);
	}

	public void initialise() {
		if (launchEmbeddedMediaDriver) {
			EmbeddedMediaDriverManager driverManager = EmbeddedMediaDriverManager.getInstance();
			driverManager.launchDriver();
			this.aeron = driverManager.getAeron();
		}
	}

	public void shutdown() {
		if (launchEmbeddedMediaDriver) {
			EmbeddedMediaDriverManager.getInstance().shutdownDriver();
		}
	}

	public Publication addPublication(String channel, int streamId) {
		return aeron.addPublication(channel, streamId);
	}

	public uk.co.real_logic.aeron.Subscription addSubscription(String channel, int streamId) {
		return aeron.addSubscription(channel, streamId);
	}

	/**
	 * Reserves a buffer claim to be used for publishing into Aeron
	 *
	 * @param publication  into which data should be published
	 * @param bufferClaim  to be used for publishing
	 * @param limit        number of bytes to be published
	 * @param idleStrategy idle strategy to use when an attempt
	 *                     to claim a buffer for publishing fails
	 * @return the reserved buffer claim or <code>null</code> when failed to
	 * claim a buffer for publishing within {@link #publicationTimeoutNs} nanos
	 */
	BufferClaim publish(Publication publication, BufferClaim bufferClaim, int limit, IdleStrategy idleStrategy) {
		long result;
		long startTime = System.nanoTime();
		while ((result = publication.tryClaim(limit, bufferClaim)) < 0) {
			if (result != Publication.BACK_PRESSURED && result != Publication.NOT_CONNECTED) {
				throw new RuntimeException("Could not publish into Aeron because of an unknown reason");
			}

			idleStrategy.idle(0);

			long now = System.nanoTime();
			if (result == Publication.NOT_CONNECTED && now - startTime > publicationTimeoutNs) {
				return null;
			}
		}

		idleStrategy.idle(1);

		return bufferClaim;
	}

	/**
	 * Wait till a message is published into Aeron. A message is considered
	 * published after {@link #publicationLingerTimeoutMillis} elapses.
	 */
	void waitLingerTimeout() {
		try {
			Thread.sleep(publicationLingerTimeoutMillis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
