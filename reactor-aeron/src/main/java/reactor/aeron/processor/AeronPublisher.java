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
import uk.co.real_logic.aeron.driver.media.UdpChannel;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Anatoly Kadyshev
 */
public class AeronPublisher implements Publisher<Buffer> {

	private final Context context;

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

	public static AeronPublisher create(Context context) {
		context.validate();
		return new AeronPublisher(context);
	}

	AeronPublisher(Context context,
				   AeronHelper aeronHelper,
				   Serializer<Throwable> exceptionSerializer,
				   Logger logger,
				   ExecutorService executor,
				   Runnable completionTask,
				   Function<Void, Boolean> processorAliveFunction,
				   Runnable onSubscribeTask) {
		this.context = context;
		this.aeronHelper = aeronHelper;
		this.exceptionSerializer = exceptionSerializer;
		this.logger = logger;
		this.executor = executor;
		this.onSubscribeTask = onSubscribeTask;
		this.shouldShutdownCreatedObjects = false;
		this.completionTask = completionTask;
		this.processorAliveFunction = processorAliveFunction;
		this.commandPub = createCommandPub(context, aeronHelper);
		this.aliveSendersChecker = createAliveSendersChecker(context, aeronHelper, logger);
	}

	public AeronPublisher(Context context) {
		this.context = context;
		this.aeronHelper = context.createAeronHelper();
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
		this.commandPub = createCommandPub(context, aeronHelper);
		this.aliveSendersChecker = createAliveSendersChecker(context, aeronHelper, logger);
	}

	private Publication createCommandPub(Context context, AeronHelper aeronHelper) {
		return aeronHelper.addPublication(context.senderChannel, context.commandRequestStreamId);
	}

	private AliveSendersChecker createAliveSendersChecker(Context context, AeronHelper aeronHelper, Logger logger) {
		return isMulticastCommunication(context) ?
				new MulticastAliveSendersChecker(logger, context, aeronHelper, commandPub) :
				new UnicastAliveSendersChecker();
	}

	private boolean isMulticastCommunication(Context context) {
		return UdpChannel.parse(context.receiverChannel).isMulticast() ||
				context.senderChannel.equals(context.receiverChannel);
	}

	@Override
	public void subscribe(Subscriber<? super Buffer> subscriber) {
		if (null == subscriber) {
			throw new NullPointerException("subscriber cannot be null");
		}

		AeronProcessorSubscription subscription = new AeronProcessorSubscription(subscriber,
				context.subscriberFragmentLimit, aeronHelper, commandPub);

		SignalsPoller signalsPoller = createSignalsPoller(subscriber, subscription);
		try {
			executor.execute(signalsPoller);

			onSubscribeTask.run();
		} catch (Exception ex) {
			logger.error("Failed to schedule poller for signals", ex);
		}
	}

	private SignalsPoller createSignalsPoller(Subscriber<? super Buffer> subscriber,
											  final AeronProcessorSubscription subscription) {
		return new SignalsPoller(context, subscriber, subscription, aeronHelper, aliveSendersChecker,
				exceptionSerializer, new Runnable() {

			@Override
			public void run() {
				if (!subscription.isActive()) {
					// Executed when subscription was cancelled

					if (context.autoCancel) {
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
