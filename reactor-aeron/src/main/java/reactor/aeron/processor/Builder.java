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

import reactor.core.support.Assert;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Builder of {@link AeronProcessor}
 */
public class Builder {

    public static final int DEFAULT_SENDER_PORT = 12000;

	public static final int DEFAULT_RECEIVER_PORT = 12001;

	/**
	 * Processor name used as a base name for threads created by
	 * the processor's executor
	 */
	String name;

	/**
	 * If the processor should cancel an upstream subscription when
	 * the last subscriber terminates
	 */
	boolean autoCancel;

    String senderChannel = createChannelForPort(DEFAULT_SENDER_PORT);

    String receiverChannel = createChannelForPort(DEFAULT_RECEIVER_PORT);

	/**
	 * Aeron StreamId used by the signals sender to publish Next and
	 * Complete signals
	 */
	int streamId = 1;

	/**
	 * Aeron StreamId used by the signals sender to publish Error signals
	 */
	int errorStreamId = 2;

	/**
	 * Aeron StreamId used by the signals sender to listen to commands
	 * from the receiver
	 */
	int commandRequestStreamId = 3;

	/**
	 * Aeron StreamId used by signals sender to reply to command requests
	 * from signals receiver
	 */
	int commandReplyStreamId = 4;

    /**
     * Instance of Aeron to be used by the processor
     */
    Aeron aeron;

	/**
	 * If embedded media driver should be launched
	 */
	boolean launchEmbeddedMediaDriver = true;

	/**
	 * Executor service used by the processor to create subscriber threads
	 */
	ExecutorService executorService;

	/**
	 * Number of fragments that could be read by the signals receiver during
     * a single call to {@link uk.co.real_logic.aeron.Subscription#poll(FragmentHandler, int)}
     * method
	 */
	int subscriberFragmentLimit;

	/**
	 * A timeout in millis after a message is considered published into Aeron.
	 */
	long publicationLingerTimeoutMillis = TimeUnit.NANOSECONDS.toMillis(Configuration.PUBLICATION_LINGER_NS);

	/**
     * A timeout during which a message is retied to be published into Aeron.
     * If the timeout elapses and the message cannot be published because of
     * either {@link uk.co.real_logic.aeron.Publication#BACK_PRESSURED} or
     * {@link uk.co.real_logic.aeron.Publication#NOT_CONNECTED} it is discarded.
     * In the next version of the processor the behaviour is likely to change.
	 */
	long publicationTimeoutMillis = 1000;

	/**
	 * Size of internal ring buffer used for processing of messages
     * to be published into Aeron
	 */
	int ringBufferSize = 1024;

	/**
	 * Delay between clean up task subsequent runs.
	 * The clean up task ignores all {@link CommandType#IsAliveReply} replies
     * published by signal senders.
	 */
	int cleanupDelayMillis = 100;

	private static String createChannelForPort(int port) {
		return "udp://localhost:" + port;
	}

	public Builder name(String name) {
		this.name = name;
		return this;
	}

	public Builder autoCancel(boolean autoCancel) {
		this.autoCancel = autoCancel;
		return this;
	}

	public Builder channel(String channel) {
		this.senderChannel = channel;
        this.receiverChannel = channel;
		return this;
	}

    public Builder senderPort(int senderPort) {
        this.senderChannel = createChannelForPort(senderPort);
        return this;
    }

	public Builder senderChannel(String senderChannel) {
		this.senderChannel = senderChannel;
		return this;
	}

	public Builder receiverPort(int receiverPort) {
		this.receiverChannel = createChannelForPort(receiverPort);
		return this;
	}

	public Builder receiverChannel(String receiverChannel) {
        this.receiverChannel = receiverChannel;
        return this;
    }

    public Builder streamId(int streamId) {
		this.streamId = streamId;
		return this;
	}

	public Builder aeron(Aeron aeron) {
		this.aeron = aeron;
		return this;
	}

	public Builder launchEmbeddedMediaDriver(boolean useEmbeddedMediaDriver) {
		this.launchEmbeddedMediaDriver = useEmbeddedMediaDriver;
		return this;
	}

	public Builder executorService(ExecutorService executorService) {
		this.executorService = executorService;
		return this;
	}

	public Builder subscriberFragmentLimit(int subscriberFragmentLimit) {
		this.subscriberFragmentLimit = subscriberFragmentLimit;
		return this;
	}

	public Builder errorStreamId(int errorStreamId) {
		this.errorStreamId = errorStreamId;
		return this;
	}

	public Builder commandRequestStreamId(int commandRequestStreamId) {
		this.commandRequestStreamId = commandRequestStreamId;
		return this;
	}

	public Builder commandReplyStreamId(int commandReplyStreamId) {
		this.commandReplyStreamId = commandReplyStreamId;
		return this;
	}

	public Builder publicationLingerTimeoutMillis(int publicationLingerTimeoutMillis) {
		this.publicationLingerTimeoutMillis = publicationLingerTimeoutMillis;
		return this;
	}

	public Builder publicationTimeoutMillis(long publicationTimeoutMillis) {
		this.publicationTimeoutMillis = publicationTimeoutMillis;
		return this;
	}

	public Builder ringBufferSize(int ringBufferSize) {
		this.ringBufferSize = ringBufferSize;
		return this;
	}

	public Builder cleanupDelayMillis(int cleanupDelayMillis) {
		this.cleanupDelayMillis = cleanupDelayMillis;
		return this;
	}

	void validate() {
		Assert.isTrue(name != null, "name should be provided");

		assertStreamIdsAreDifferent();

        if (launchEmbeddedMediaDriver) {
            Assert.isTrue(aeron == null, "aeron should be null when launchEmbeddedMediaDriver is true");
        } else {
            Assert.isTrue(aeron != null, "aeron should provided when launchEmbeddedMediaDriver is false");
        }
	}

	private void assertStreamIdsAreDifferent() {
		Set<Integer> streamIdsSet = new HashSet<>();
		streamIdsSet.add(streamId);
		streamIdsSet.add(errorStreamId);
		streamIdsSet.add(commandRequestStreamId);
		streamIdsSet.add(commandReplyStreamId);

		Assert.isTrue(streamIdsSet.size() == 4,
				String.format("streamId: %d, errorStreamId: %d, commandRequestStreamId: %d, commandReplyStreamId: %d "
								+ "should all be different",
						streamId, errorStreamId, commandRequestStreamId, commandReplyStreamId));
	}

	AeronHelper createAeronHelper() {
		AeronHelper aeronHelper = new AeronHelper(aeron, launchEmbeddedMediaDriver,
				publicationTimeoutMillis, publicationLingerTimeoutMillis);
		aeronHelper.initialise();
		return aeronHelper;
	}

}
