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

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.ExecutorService;

/**
 * @author Anatoly Kadyshev
 */
class CommandsPoller implements Runnable {

	private final Logger logger;

	private final uk.co.real_logic.aeron.Subscription commandsSub;

	private final Publication replyPub;

	private final AeronHelper aeronHelper;

	private volatile boolean running;

	private volatile Subscription upstreamSubscription;

	public void setUpstreamSubscription(Subscription upstreamSubscription) {
		this.upstreamSubscription = upstreamSubscription;
	}

	private class PollerFragmentHandler implements FragmentHandler {

		@Override
		public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
			if (!running) {
				return;
			}

			byte command = buffer.getByte(offset);
			if (command == CommandType.Request.getCode()) {
				long n = buffer.getLong(offset + 1);
				if (upstreamSubscription != null) {
					upstreamSubscription.request(n);
				}
			} else if (command == CommandType.Cancel.getCode()) {
				if (upstreamSubscription != null) {
					upstreamSubscription.cancel();
				}
			} else if (command == CommandType.IsAliveRequest.getCode()) {
				handleIsAliveRequest(buffer.getLong(offset + 1), buffer.getLong(offset + 9));
			} else if (command == CommandType.IsAliveReply.getCode()) {
				// Skip
			} else {
				logger.error("Unknown command code: {} received", command);
			}
		}

		void handleIsAliveRequest(long mostSignificantBits, long leastSignificantBits) {
			IdleStrategy idleStrategy = AeronHelper.newBackoffIdleStrategy();

			BufferClaim bufferClaim = aeronHelper.publish(replyPub, new BufferClaim(), 1 + 16, idleStrategy);
			if (bufferClaim != null) {
				try {
					int offset = bufferClaim.offset();
					MutableDirectBuffer buffer = bufferClaim.buffer();
					buffer.putByte(offset, CommandType.IsAliveReply.getCode());
					AeronHelper.putUUID(buffer, offset + 1, mostSignificantBits, leastSignificantBits);
				} finally {
					bufferClaim.commit();
				}
			}
		}
	}

	CommandsPoller(Logger logger, AeronHelper aeronHelper, int commandRequestStreamId, int commandReplyStreamId) {
		this.logger = logger;
		this.aeronHelper = aeronHelper;
		this.commandsSub = aeronHelper.addSubscription(commandRequestStreamId);
		this.replyPub = aeronHelper.addPublication(commandReplyStreamId);
	}

	void initialize(ExecutorService executorService) {
		executorService.execute(this);
	}

	public void run() {
		this.running = true;

		IdleStrategy idleStrategy = AeronHelper.newBackoffIdleStrategy();

		FragmentAssembler fragmentAssembler = new FragmentAssembler(new PollerFragmentHandler());

		while (running) {
			int nFragmentsReceived = commandsSub.poll(fragmentAssembler, 1);
			idleStrategy.idle(nFragmentsReceived);
		}

	}

	void shutdown() {
		this.running = false;
	}
}
