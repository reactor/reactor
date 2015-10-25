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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import reactor.Timers;
import reactor.core.subscriber.SerializedSubscriber;
import reactor.core.support.UUIDUtils;
import reactor.fn.Consumer;
import reactor.fn.Pausable;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 * Checks if there is any alive Aeron signals sender.
 *
 * @author Anatoly Kadyshev
 */
class AliveSendersChecker {

	private final uk.co.real_logic.aeron.Subscription commandsSub;

	private final Logger logger;

	private final AeronHelper aeronHelper;

	private final Publication commandsPub;

	private final long publicationLingerTimeoutMillis;

    private final Pausable cleanupTaskPausable;

	private volatile boolean scheduled = false;

	private volatile boolean allDead = false;

	private final FragmentHandler cleanupFragmentHandler = new FragmentHandler() {
		@Override
		public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
		}
	};

	private static final Object CLEANUP = new Object();

	private static final Object CHECK_ALIVE_SENDERS = new Object();

	private final SerializedSubscriber<Object> serializedSubscriber =
            SerializedSubscriber.create(new Subscriber<Object>() {
		@Override
		public void onSubscribe(Subscription s) {
		}

		@Override
		public void onNext(Object o) {
			if (o == CLEANUP) {
				cleanup();
			} else if (o == CHECK_ALIVE_SENDERS) {
				checkAliveSenders();
			}
		}

		@Override
		public void onError(Throwable t) {
		}

		@Override
		public void onComplete() {
		}
	});

    AliveSendersChecker(Logger logger, AeronHelper aeronHelper, Publication commandsPub,
                        String senderChannel,
						int commandReplyStreamId, long publicationLingerTimeoutMillis, int cleanupDelayMillis) {
		this.logger = logger;
		this.aeronHelper = aeronHelper;
		this.commandsPub = commandsPub;
		this.publicationLingerTimeoutMillis = publicationLingerTimeoutMillis;
		this.commandsSub = aeronHelper.addSubscription(senderChannel, commandReplyStreamId);

        cleanupTaskPausable = Timers.global().schedule(new Consumer<Long>() {
            @Override
            public void accept(Long value) {
                serializedSubscriber.onNext(CLEANUP);
            }
        }, cleanupDelayMillis, TimeUnit.MILLISECONDS);
    }

	void scheduleCheck() {
		if (!scheduled) {
			scheduled = true;
			serializedSubscriber.onNext(CHECK_ALIVE_SENDERS);
		}
	}

	void shutdown() {
        cleanupTaskPausable.cancel();
		commandsSub.close();
	}

	void cleanup() {
        try {
            int nFragmentsReceived;
            long startTime = System.nanoTime();
            do {
                nFragmentsReceived = commandsSub.poll(cleanupFragmentHandler, 100);
            } while (nFragmentsReceived > 0 &&
                    (System.nanoTime() - startTime < TimeUnit.MILLISECONDS.toMillis(50)));
        } catch (Exception e) {
            logger.error(this + " - Failed to cleanup");
        }
	}

    class AliveCountingFragmentHandler implements FragmentHandler {

        volatile int aliveSendersCounter = 0;

        FragmentHandler delegate;

        AliveCountingFragmentHandler(UUID id) {
            this.delegate = new FragmentAssembler(new FragmentHandler() {
                @Override
                public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                    byte command = buffer.getByte(offset);
                    if (command == CommandType.IsAliveReply.getCode()) {
                        if (id.getMostSignificantBits() == buffer.getLong(offset + 1) &&
                                id.getLeastSignificantBits() == buffer.getLong(offset + 9)) {
                            aliveSendersCounter++;
                        }
                    }
                }
            });
        }

        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
            delegate.onFragment(buffer, offset, length, header);
        }

        public int getAliveSendersCounter() {
            return aliveSendersCounter;
        }
    }

	void checkAliveSenders() {
		scheduled = false;

		final UUID id = sendRequestAlive();
		if (id == null) {
			allDead = true;
            if (logger.isDebugEnabled()) {
                logger.info("No alive senders detected");
            }
			return;
		}

        AliveCountingFragmentHandler fragmentHandler = new AliveCountingFragmentHandler(id);

        final IdleStrategy idleStrategy = AeronHelper.newBackoffIdleStrategy();
        final long waitForReplyNs = TimeUnit.MILLISECONDS.toNanos(publicationLingerTimeoutMillis * 2);
		long startTime = System.nanoTime();
		do {
			int nFragmentsReceived = commandsSub.poll(fragmentHandler, 100);
			idleStrategy.idle(nFragmentsReceived);
		} while (System.nanoTime() - startTime < waitForReplyNs);

        allDead = fragmentHandler.getAliveSendersCounter() == 0;

		if (allDead) {
			if (logger.isDebugEnabled()) {
				logger.info("No alive senders detected");
			}
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("{} alive senders detected", fragmentHandler.getAliveSendersCounter());
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
