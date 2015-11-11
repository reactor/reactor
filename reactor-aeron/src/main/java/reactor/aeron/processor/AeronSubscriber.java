/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import org.slf4j.LoggerFactory;
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.RingBufferProcessor;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriber implements Subscriber<Buffer> {

	private final RingBufferProcessor<Buffer> delegateProcessor;

	private final CommandsPoller commandsPoller;

	private final AtomicBoolean alive = new AtomicBoolean(true);

	private final Runnable postShutdownTask;

	private final ExecutorService executor;

	private final boolean shouldShutdownCreatedObjects;

	private final AeronHelper aeronHelper;

	class RingBufferProcessorSubscriber implements Subscriber<Buffer> {

		private final Publication nextCompletePub;

		private final BufferClaim bufferClaim;

		private final IdleStrategy idleStrategy;

		private final Publication errorPub;

		private final AeronHelper aeronHelper;

		private final Serializer<Throwable> exceptionSerializer;

		RingBufferProcessorSubscriber(Context context,
									  AeronHelper aeronHelper,
									  Serializer<Throwable> exceptionSerializer) {
			this.aeronHelper = aeronHelper;
			this.exceptionSerializer = exceptionSerializer;
			this.nextCompletePub = aeronHelper.addPublication(context.receiverChannel, context.streamId);
			this.errorPub = aeronHelper.addPublication(context.receiverChannel, context.errorStreamId);
			this.bufferClaim = new BufferClaim();
			this.idleStrategy = AeronHelper.newBackoffIdleStrategy();
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Buffer buffer) {
			if (!publishSignal(nextCompletePub, buffer, SignalType.Next, false)) {
				//TODO: Handle Aeron publication backpressured situation
			}
		}

		@Override
		public void onError(Throwable t) {
			Buffer buffer = Buffer.wrap(exceptionSerializer.serialize(t));

			publishSignal(errorPub, buffer, SignalType.Error, true);

			shutdown();
		}

		@Override
		public void onComplete() {
			Buffer buffer = new Buffer(0, true);
			publishSignal(nextCompletePub, buffer, SignalType.Complete, true);

			shutdown();
		}

		boolean publishSignal(Publication publication, Buffer buffer, SignalType signalType,
							  boolean waitLingerTimeout) {
			BufferClaim claim = aeronHelper.publish(publication, bufferClaim, buffer.limit() + 1, idleStrategy);
			if (claim != null) {
				try {
					buffer.byteBuffer().mark();

					MutableDirectBuffer mutableBuffer = claim.buffer();
					int offset = bufferClaim.offset();
					mutableBuffer.putByte(offset, signalType.getCode());
					mutableBuffer.putBytes(offset + 1, buffer.byteBuffer(), buffer.limit());

					buffer.byteBuffer().reset();
				} finally {
					claim.commit();
				}
				if (waitLingerTimeout) {
					aeronHelper.waitLingerTimeout();
				}
				return true;
			}
			return false;
		}
	}

	public static AeronSubscriber create(Context context) {
		context.validate();
		return new AeronSubscriber(context, false);
	}

	public static AeronSubscriber share(Context context) {
		context.validate();
		return new AeronSubscriber(context, true);
	}

	public AeronSubscriber(Context context,
						   AeronHelper aeronHelper,
						   ExecutorService executor,
						   Serializer<Throwable> exceptionSerializer,
						   Logger logger,
						   boolean multiPublishers,
						   Runnable postShutdownTask) {
		this.aeronHelper = aeronHelper;
		this.postShutdownTask = postShutdownTask;
		this.executor = executor;
		this.shouldShutdownCreatedObjects = false;

		this.delegateProcessor = createProcessor(context, multiPublishers);
		subscribeProcessor(context, aeronHelper, exceptionSerializer);

		this.commandsPoller = createCommandsPoller(context, aeronHelper, logger);
		commandsPoller.initialize(executor);
	}

	private CommandsPoller createCommandsPoller(Context context, AeronHelper aeronHelper, Logger logger) {
		return new CommandsPoller(logger, context, aeronHelper);
	}

	private void subscribeProcessor(Context context, AeronHelper aeronHelper,
									Serializer<Throwable> exceptionSerializer) {
		delegateProcessor.subscribe(new RingBufferProcessorSubscriber(context, aeronHelper, exceptionSerializer));
	}

	public AeronSubscriber(Context context, boolean multiPublishers) {
		this.aeronHelper = context.createAeronHelper();
		this.postShutdownTask = new Runnable() {
			@Override
			public void run() {
			}
		};
		this.executor = Executors.newCachedThreadPool();

		this.delegateProcessor = createProcessor(context, multiPublishers);
		subscribeProcessor(context, aeronHelper, new BasicExceptionSerializer());

		this.commandsPoller = createCommandsPoller(context, aeronHelper,
				LoggerFactory.getLogger(AeronSubscriber.class));
		commandsPoller.initialize(executor);

		this.shouldShutdownCreatedObjects = true;
	}

	RingBufferProcessor<Buffer> createProcessor(Context context, boolean multiPublishers) {
		RingBufferProcessor<Buffer> processor;
		if (multiPublishers) {
			processor = RingBufferProcessor.share(context.name + "-ring-buffer-consumer", context.ringBufferSize);
		} else {
			processor = RingBufferProcessor.create(context.name + "-ring-buffer-consumer", context.ringBufferSize);
		}
		return processor;
	}

	@Override
	public void onSubscribe(Subscription s) {
		commandsPoller.setUpstreamSubscription(s);
	}

	@Override
	public void onNext(Buffer buffer) {
		if (buffer == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}
		delegateProcessor.onNext(buffer);
	}

	@Override
	public void onError(Throwable t) {
		if (t == null) {
			throw new NullPointerException("Error could not be null");
		}
		delegateProcessor.onError(t);
	}

	@Override
	public void onComplete() {
		delegateProcessor.onComplete();
	}

	void shutdown() {
		if (alive.compareAndSet(true, false)) {
			delegateProcessor.shutdown();
			commandsPoller.shutdown();

			if (shouldShutdownCreatedObjects) {
				executor.shutdown();
				aeronHelper.shutdown();
			}

			postShutdownTask.run();
		}
	}
}
