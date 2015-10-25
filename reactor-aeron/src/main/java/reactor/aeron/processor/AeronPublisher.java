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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Anatoly Kadyshev
 */
public class AeronPublisher implements Publisher<Buffer> {

	private final Builder builder;

	private final AeronHelper aeronHelper;

	private final Serializer<Throwable> exceptionSerializer;

	private final Logger logger;

	private final ExecutorService executor;

	private final Runnable onSubscribeTask;

	private final Runnable completionTask;

	private final Function<Void, Boolean> processorAliveFunction;

	private final Publication commandPub;

	private final AtomicBoolean alive = new AtomicBoolean(true);

	private final AliveSendersChecker aliveSendersChecker;

	private final boolean shouldShutdownCreatedObjects;

	public static AeronPublisher create(Builder builder) {
		builder.validate();
		return new AeronPublisher(builder);
	}

	AeronPublisher(Builder builder,
				   AeronHelper aeronHelper,
				   Serializer<Throwable> exceptionSerializer,
				   Logger logger,
				   ExecutorService executor,
				   Runnable completionTask,
				   Function<Void, Boolean> processorAliveFunction,
				   Runnable onSubscribeTask) {
		this.builder = builder;
		this.aeronHelper = aeronHelper;
		this.exceptionSerializer = exceptionSerializer;
		this.logger = logger;
		this.executor = executor;
		this.onSubscribeTask = onSubscribeTask;
		this.shouldShutdownCreatedObjects = false;
		this.completionTask = completionTask;
		this.processorAliveFunction = processorAliveFunction;
		this.commandPub = aeronHelper.addPublication(builder.receiverChannel, builder.commandRequestStreamId);
		this.aliveSendersChecker = createAliveSendersChecker(builder, aeronHelper, logger);
	}

	private AliveSendersChecker createAliveSendersChecker(Builder builder, AeronHelper aeronHelper, Logger logger) {
		return new AliveSendersChecker(logger, aeronHelper, commandPub,
				builder.senderChannel, builder.commandReplyStreamId,
				builder.publicationLingerTimeoutMillis, builder.cleanupDelayMillis);
	}

	public AeronPublisher(Builder builder) {
		this.builder = builder;
		this.aeronHelper = builder.createAeronHelper();
		this.exceptionSerializer = new BasicExceptionSerializer();
		this.logger = LoggerFactory.getLogger(AeronPublisher.class);
		this.executor = Executors.newCachedThreadPool();
		this.shouldShutdownCreatedObjects = true;
		this.completionTask = new Runnable() {
			@Override
			public void run() {
				shutdown();
			}
		};
		this.onSubscribeTask = new Runnable() {
			@Override
			public void run() {
			}
		};
		this.processorAliveFunction = new Function<Void, Boolean>() {
			@Override
			public Boolean apply(Void aVoid) {
				return false;
			}
		};
		this.commandPub = aeronHelper.addPublication(builder.receiverChannel, builder.commandRequestStreamId);
		this.aliveSendersChecker = createAliveSendersChecker(builder, aeronHelper, logger);
	}

	@Override
	public void subscribe(Subscriber<? super Buffer> subscriber) {
		if (null == subscriber) {
			throw new NullPointerException("subscriber cannot be null");
		}

		AeronProcessorSubscription subscription = new AeronProcessorSubscription(subscriber,
				builder.subscriberFragmentLimit, aeronHelper, commandPub);

		SignalsPoller signalsPoller = createSignalsPoller(subscriber, subscription);
		try {
			executor.execute(signalsPoller);

			onSubscribeTask.run();
		} catch (Exception ex) {
			logger.error("Failed to schedule poller for signals", ex);
		}
	}

	private SignalsPoller createSignalsPoller(Subscriber<? super Buffer> subscriber, final AeronProcessorSubscription subscription) {
		return new SignalsPoller(subscriber, subscription, aeronHelper, builder.senderChannel,
				aliveSendersChecker, exceptionSerializer, builder.streamId, builder.errorStreamId, new Runnable() {

			@Override
			public void run() {
				if (!subscription.isActive()) {
					// Executed when subscription was cancelled

					if (builder.autoCancel) {
						sendCancelCommand();
					}
				}

				completionTask.run();
			}

			void sendCancelCommand() {
				BufferClaim bufferClaim = aeronHelper.publish(commandPub, new BufferClaim(), 9,
						AeronHelper.newBackoffIdleStrategy());
				if (bufferClaim != null) {
					try {
						MutableDirectBuffer mutableBuffer = bufferClaim.buffer();
						int offset = bufferClaim.offset();
						mutableBuffer.putByte(offset, CommandType.Cancel.getCode());
					} finally {
						bufferClaim.commit();
					}
					aeronHelper.waitLingerTimeout();
				}
			}
		}, processorAliveFunction);
	}

	boolean shutdown() {
		if (alive.compareAndSet(true, false)) {
			aliveSendersChecker.shutdown();
			commandPub.close();
			if (shouldShutdownCreatedObjects) {
				executor.shutdown();
				aeronHelper.shutdown();
			}
			return true;
		}
		return false;
	}

}
