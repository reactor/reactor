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

	/**
	 * Aeron channel used by the signals sender and the receiver
	 * of the processor
	 */
	String channel;

	/**
	 * Aeron StreamId used by the signals sender to publish Next and
	 * Complete signals
	 */
	Integer streamId;

	/**
	 * Aeron StreamId used by the signals sender to publish Error signals
	 */
	Integer errorStreamId;

	/**
	 * Aeron StreamId used by the signals sender to listen to commands
	 * from the receiver
	 */
	Integer commandRequestStreamId;

	/**
	 * Aeron StreamId used by signals sender to reply to command requests
	 * from signals receiver
	 */
	Integer commandReplyStreamId;

	/**
	 * Context for publishing signals into Aeron
	 * When <tt>null</tt> a new one is initialized by the processor
	 */
	Aeron.Context signalSenderContext;

	/**
	 * Context for reading signals from Aeron
	 * When <tt>null</tt> a new one is initialized by the processor
	 */
	Aeron.Context signalReceiverContext;

	/**
	 * If embedded media driver should be launched
	 */
	boolean launchEmbeddedMediaDriver = true;

	/**
	 * Executor service used by the processor to create subscriber threads
	 */
	ExecutorService executorService;

	/**
	 * If publishing from multiple threads should be supported
	 */
	boolean multiPublishers;

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

	Builder() {
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
		this.channel = channel;
		return this;
	}

	public Builder streamId(int streamId) {
		this.streamId = streamId;
		return this;
	}

	public Builder signalSenderContext(Aeron.Context signalSenderContext) {
		this.signalSenderContext = signalSenderContext;
		return this;
	}

	public Builder signalReceiverContext(Aeron.Context signalReceiverContext) {
		this.signalReceiverContext = signalReceiverContext;
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

	/**
	 * Creates a new processor supporting a single publishing thread only
     * using the builder fields.
	 *
	 * @return a new processor
	 */
	public AeronProcessor create() {
		validate();
		return new AeronProcessor(this);
	}

	/**
	 * Creates a new processor supports publishing from multiple threads
     * using the builder fields.
	 *
	 * @return a new processor
	 */
	public AeronProcessor share() {
		this.multiPublishers = true;
		validate();
		return new AeronProcessor(this);
	}

	private void validate() {
		Assert.isTrue(name != null, "name should be provided");
		Assert.isTrue(channel != null, "channel should be provided");
		assertStreamIdsAreDifferent();
	}

	private void assertStreamIdsAreDifferent() {
		Assert.notNull(streamId, "streamId should be provided");
		Assert.notNull(errorStreamId, "errorStreamId should be provided");
		Assert.notNull(commandRequestStreamId, "commandRequestStreamId should be provided");
		Assert.notNull(commandReplyStreamId, "commandReplyStreamId should be provided");

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
}
