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
 * @author Anatoly Kadyshev
 */
class AeronHelper {

	private final Aeron.Context ctx;

	private final String channel;

	private final boolean launchEmbeddedMediaDriver;

	private final long publicationLingerTimeoutMillis;

	private Aeron aeron;

	/**
	 * How long to wait for Subscriber for a publication before giving up publishing data into it
	 */
	private final long waitForSubscriberMillis;

	AeronHelper(Aeron.Context ctx, boolean launchEmbeddedMediaDriver,
				String channel, long waitForSubscriberMillis,
				long publicationLingerTimeoutMillis) {
		this.launchEmbeddedMediaDriver = launchEmbeddedMediaDriver;
		this.publicationLingerTimeoutMillis = publicationLingerTimeoutMillis;
		if (ctx == null) {
			ctx = new Aeron.Context();
		}
		if (launchEmbeddedMediaDriver) {
			EmbeddedMediaDriverManager driverManager = EmbeddedMediaDriverManager.getInstance();
			driverManager.launchDriver();
			ctx.dirName(driverManager.getDriver().contextDirName());
		}

		this.ctx = ctx;
		this.channel = channel;
		this.waitForSubscriberMillis = waitForSubscriberMillis;
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

	void initialise() {
		this.aeron = Aeron.connect(ctx);
	}

	void shutdown() {
		aeron.close();

		if (launchEmbeddedMediaDriver) {
			EmbeddedMediaDriverManager.getInstance().shutdownDriver();
		}
	}

	Publication addPublication(int streamId) {
		return aeron.addPublication(channel, streamId);
	}

	uk.co.real_logic.aeron.Subscription addSubscription(int streamId) {
		return aeron.addSubscription(channel, streamId);
	}

	/**
	 * Reserves a buffer claim to be used for publishing into Aeron
	 *
	 * @param publication  into which data should be published
	 * @param bufferClaim  to be used for publishing
	 * @param limit        number of bytes to be published
	 * @param idleStrategy idle strategy to use when claim for data publishing fails
	 * @return the reserved buffer claim or <code>null</code> when no subscribers are connected
	 */
	BufferClaim publish(Publication publication, BufferClaim bufferClaim, int limit, IdleStrategy idleStrategy) {
		long result;
		final long timeoutNs = TimeUnit.MILLISECONDS.toNanos(waitForSubscriberMillis);
		long startTime = System.nanoTime();
		while ((result = publication.tryClaim(limit, bufferClaim)) < 0) {
			if (result != Publication.BACK_PRESSURED && result != Publication.NOT_CONNECTED) {
				throw new RuntimeException("Could not publish into Aeron because of an unknown reason");
			}
			idleStrategy.idle(0);

			if (System.nanoTime() - startTime > timeoutNs) {
				// TODO: Rethink handling back-pressured publication
				return null;
			}
		}
		return bufferClaim;
	}

	void waitLingerTimeout() {
		try {
			Thread.sleep(publicationLingerTimeoutMillis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
