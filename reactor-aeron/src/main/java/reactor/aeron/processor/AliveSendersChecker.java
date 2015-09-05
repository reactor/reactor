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

import org.slf4j.Logger;
import reactor.core.support.UUIDUtils;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * Checks if there is any alive Aeron signals sender.
 *
 * @author Anatoly Kadyshev
 */
class AliveSendersChecker {

	private final uk.co.real_logic.aeron.Subscription commandsSub;

	private final ScheduledExecutorService executorService;

	private final Logger logger;

	private final AeronHelper aeronHelper;

	private final Publication commandsPub;

	private final long publicationLingerTimeoutMillis;

	private volatile boolean scheduled = false;

	private volatile boolean allDead = false;

	private final FragmentHandler cleanupFragmentHandler = new FragmentHandler() {
		@Override
		public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
		}
	};

	private final Runnable checkAlivePublishersTask = new Runnable() {
		@Override
		public void run() {
			checkAliveSenders();
		}
	};

	private final Runnable cleanupTask = new Runnable() {
		@Override
		public void run() {
			cleanup();
		}
	};

	AliveSendersChecker(Logger logger, AeronHelper aeronHelper, Publication commandsPub,
						int commandReplyStreamId, long publicationLingerTimeoutMillis) {
		this.logger = logger;
		this.aeronHelper = aeronHelper;
		this.commandsPub = commandsPub;
		this.publicationLingerTimeoutMillis = publicationLingerTimeoutMillis;
		this.executorService = Executors.newSingleThreadScheduledExecutor();
		this.commandsSub = aeronHelper.addSubscription(commandReplyStreamId);

		//TODO: Move hard-coded value into configuration
		this.executorService.scheduleWithFixedDelay(cleanupTask, 100, 100, TimeUnit.MILLISECONDS);
	}

	void scheduleCheck() {
		if (!scheduled) {
			scheduled = true;
			executorService.submit(checkAlivePublishersTask);
		}
	}

	void shutdown() {
		executorService.shutdown();
		commandsSub.close();
	}

	void cleanup() {
		int nFragmentsReceived;
		long startTime = System.nanoTime();
		do {
			nFragmentsReceived = commandsSub.poll(cleanupFragmentHandler, 100);
		} while (nFragmentsReceived > 0 &&
				(System.nanoTime() - startTime < TimeUnit.MILLISECONDS.toMillis(50)));
	}

	void checkAliveSenders() {
		scheduled = false;

		final UUID id = sendRequestAlive();
		if (id == null) {
			allDead = true;
			return;
		}

		final IdleStrategy idleStrategy = AeronHelper.newBackoffIdleStrategy();

		final int[] nAlivePublishers = {0};
		FragmentHandler fragmentHandler = new FragmentAssembler(new FragmentHandler() {
			@Override
			public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
				byte command = buffer.getByte(offset);
				if (command == CommandType.IsAliveReply.getCode()) {
					if (id.getMostSignificantBits() == buffer.getLong(offset + 1) &&
							id.getLeastSignificantBits() == buffer.getLong(offset + 9)) {
						nAlivePublishers[0]++;
					}
				}
			}
		});

		final long waitForReplyNs = TimeUnit.MILLISECONDS.toNanos(publicationLingerTimeoutMillis * 2);
		long startTime = System.nanoTime();
		do {
			int nFragmentsReceived = commandsSub.poll(fragmentHandler, 100);
			idleStrategy.idle(nFragmentsReceived);
		} while (System.nanoTime() - startTime < waitForReplyNs);

		if (nAlivePublishers[0] == 0) {
			allDead = true;
			if (logger.isDebugEnabled()) {
				logger.info("No alive publishers detected");
			}
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("{} alive publishers detected", nAlivePublishers[0]);
			}
		}
	}

	UUID sendRequestAlive() {
		BufferClaim bufferClaim = aeronHelper.publish(commandsPub, new BufferClaim(), 1 + 8 + 8,
				AeronHelper.newBackoffIdleStrategy());
		UUID id = null;
		if (bufferClaim != null) {
			try {
				id = UUIDUtils.create();
				MutableDirectBuffer mutableBuffer = bufferClaim.buffer();
				int offset = bufferClaim.offset();
				mutableBuffer.putByte(offset, CommandType.IsAliveRequest.getCode());
				AeronHelper.putUUID(mutableBuffer, offset + 1, id.getMostSignificantBits(),
						id.getLeastSignificantBits());
			} finally {
				bufferClaim.commit();
			}
		}
		return id;
	}

	public boolean isAllDead() {
		return allDead;
	}
}
