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
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.ExecutorPoweredProcessor;
import reactor.core.processor.RingBufferProcessor;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A processor which publishes into and subscribes to data from Aeron.<br>
 * For more information about Aeron go to
 * <a href="https://github.com/real-logic/Aeron">Aeron Project Home</a>
 *
 * <p>The processor plays roles of both {@link Publisher} and
 * {@link Subscriber}<br>
 * <ul>
 * <li>{@link Subscriber} part of the processor called as
 * <b>'signals sender'</b> below publishes messages into Aeron.</li>
 * <li>{@link Publisher} part of the processor called as
 * <b>'signals receiver'</b> below subscribers for messages published
 * by the sender part.</li>
 * </ul>
 * </p>
 *
 * <p>An instance of the processor is created upon a single Aeron channel of
 * {@link #channel} and requires 4 <b>different</b> streamIds to function:<br>
 * <ul>
 *     <li>{@link #streamId} - used for sending Next and Complete signals from
 *     the signals sender to the signals receiver</li>
 *     <li>{@link #errorStreamId} - for Error signals</li>
 *     <li>{@link #commandRequestStreamId} - for {@link CommandType#Request},
 *     {@link CommandType#Cancel} and {@link CommandType#IsAliveRequest}</li>
 *     from the signals receiver to the signals sender
 *     <li>{@link #commandReplyStreamId} - for command execution results from
 *     the signals sender to the signals receiver</li>
 * </ul>
 * </p>
 *
 * <p>The processor could launch an embedded Media Driver for the application
 * if requested via <code>launchEmbeddedMediaDriver</code> parameter during
 * the processor creation via static methods or via
 * {@link Builder#launchEmbeddedMediaDriver(boolean)} when created using the
 * {@link Builder}.<br>
 * Only a single instance of the embedded media driver is launched for the
 * application.<br>
 * The launched Media Driver instance is shut down once the last
 * instance of {@link AeronProcessor} is shut down.
 *
 * <p>The processor created via {@link #create(String, boolean, boolean, String, int, int, int, int)}
 * or {@link Builder#create()} methods respects the Reactive Streams contract
 * and must not be signalled concurrently on any onXXXX methods.<br>
 * Nonetheless Reactor allows creating of a processor which can be used by
 * publishers from different threads. In this case the processor should be
 * created via either {@link #share(String, boolean, boolean, String, int, int, int, int)}
 * or {@link Builder#share()} methods.<br>
 *
 * <p>Each subscriber is assigned a unique thread that stops either on
 * the processor subscription cancellation or upon a terminal event of Complete or Error.
 * </p>
 *
 * <p>When auto-cancel is enabled and the last subscriber is unregistered
 * an upstream subscription to the upstream publisher is cancelled.</p>
 *
 * <p>The processor could be assigned a custom executor service when is
 * constructed via {@link Builder}. The executor service decides upon threads
 * allocation for the processor subscribers.</p>
 *
 * <p>When a Subscriber to the processor requests {@link Long#MAX_VALUE} there
 * won't be any backpressure applied and thread publishing into the processor
 * will run at risk of being throttled if subscribers don't catch up.<br>
 * With any other strictly positive demand a subscriber will stop reading new
 * Next signals (Complete and Error will still be read) as soon as the demand
 * has been fully consumed.</p>
 *
 * <p>When more than 1 subscriber listens to the processor they all receive
 * the exact same events if their respective demand is still strictly positive,
 * very much like a Fan-Out scenario.</p>
 *
 * <p>When the Aeron buffer for published messages becomes completely full
 * the processor starts to throttle and as a result method
 * {@link #onNext(Buffer)} blocks until messages are consumed or
 * {@link #publicationTimeoutMillis} timeout elapses.<br>
 *
 * If a message cannot be published into Aeron within
 * {@link #publicationTimeoutMillis} then it is discarded.
 * In the next version of the processor this behaviour is likely to change.<br>
 *
 * For configuration of Aeron buffers refer to
 * <a href="https://github.com/real-logic/Aeron/wiki/Configuration-Options">Aeron Configuration Options</a>
 * </p>
 *
 * @author Anatoly Kadyshev
 */
public class AeronProcessor extends ExecutorPoweredProcessor<Buffer, Buffer> {

	private static final Logger logger = LoggerFactory.getLogger(AeronProcessor.class);

    /**
     * Described at {@link Builder#channel}
     */
	private final String channel;

    /**
     * Described at {@link Builder#streamId}
     */
	private final int streamId;

    /**
     * Described at {@link Builder#errorStreamId}
     */
	private final int errorStreamId;

    /**
     * Described at {@link Builder#commandRequestStreamId}
     */
	private final int commandRequestStreamId;

    /**
     * Described at {@link Builder#commandReplyStreamId}
     */
	private final int commandReplyStreamId;

    /**
     * Described at {@link Builder#publicationLingerTimeoutMillis}
     */
	private final long publicationLingerTimeoutMillis;

    /**
     * Described at {@link Builder#publicationTimeoutMillis}
     */
    private final long publicationTimeoutMillis;

    /**
     * Exception serializer to serialize Error messages sent into Aeron
     */
	private final Serializer<Throwable> exceptionSerializer;

    /**
     * Reactive Publisher part of the processor - signals sender
     */
	private final AeronProcessorPublisher publisher;

    /**
     * Reactive Subscriber part of the processor - signals receiver
     */
	private final AeronProcessorSubscriber subscriber;

    /**
     * Signals receiver functionality which polls for signals sent by senders
     */
	private class SignalsPoller implements Runnable {

		private final Subscriber<? super Buffer> subscriber;

		private final AeronProcessorSubscription processorSubscription;

		private final AeronHelper aeronHelper;

		private final Publication commandPub;

		private final AliveSendersChecker aliveSendersChecker;

		private final Runnable completionTask;

		/**
		 * Complete signal was received from one of senders
		 */
		private volatile boolean completeReceived = false;

		/**
		 * Error signal was received from one of senders
		 */
		private volatile boolean errorReceived = false;

		private abstract class SignalsPollerFragmentHandler implements FragmentHandler {

			@Override
			public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
				// terminal event was received => all other Next and Complete events should be ignored
				if (errorReceived) {
					return;
				}

				byte[] data = new byte[length - 1];
				buffer.getBytes(offset + 1, data);
				byte signalTypeCode = buffer.getByte(offset);
				try {
					if (!handleSignal(signalTypeCode, data, header.sessionId())) {
						logger.error("Message with unknown signal type code of {} and length of {} was ignored",
								signalTypeCode, data.length);
					}
				} catch (Throwable t) {
					Exceptions.throwIfFatal(t);
					subscriber.onError(t);
				}
			}

			/**
			 * Handles signal with type code of <code>signalTypeCode</code> and
             * content of <code>data</code>
			 *
			 * @param signalTypeCode signal type code
			 * @param data signal data
			 * @param sessionId Aeron sessionId
			 * @return true if signal was handled and false otherwise
			 */
			abstract boolean handleSignal(byte signalTypeCode, byte[] data, int sessionId);

		}

        /**
         * Handler for Complete and Next signals
         */
		private class CompleteNextFragmentHandler extends SignalsPollerFragmentHandler {

			/**
			 * If should read a single message from Aeron.
			 * Used to check if Complete signal was sent before any events were
             * requested via a subscription
			 */
			boolean shouldSnap;

            /**
             * Message received from Aeron when {link #shouldSnap} was set
             */
			Buffer snappedNextMsg;

            /**
             * Number of Next signals received
             */
			int nNextSignalsReceived;

			@Override
			boolean handleSignal(byte signalTypeCode, byte[] data, int sessionId) {
				if (signalTypeCode == SignalType.Next.getCode()) {
					Buffer buffer = Buffer.wrap(data);
					if (shouldSnap) {
						snappedNextMsg = buffer;
					} else {
						subscriber.onNext(buffer);
						nNextSignalsReceived++;
					}
				} else if (signalTypeCode == SignalType.Complete.getCode()) {
					completeReceived = true;
				} else {
					return false;
				}
				return true;
			}

			int getAndClearNextEventsReceived() {
				int result = nNextSignalsReceived;
				nNextSignalsReceived = 0;
				return result;
			}

		}

        /**
         * Handler for Error signals
         */
		private class ErrorFragmentHandler extends SignalsPollerFragmentHandler {

			@Override
			boolean handleSignal(byte signalTypeCode, byte[] data, int sessionId) {
				if (signalTypeCode == SignalType.Error.getCode()) {
					Throwable t = exceptionSerializer.deserialize(data);
					subscriber.onError(t);

					errorReceived = true;
					return true;
				}
				return false;
			}

		}

		public SignalsPoller(Subscriber<? super Buffer> subscriber,
							 AeronProcessorSubscription processorSubscription,
							 AeronHelper aeronHelper, Publication commandPub,
							 AliveSendersChecker aliveSendersChecker,
							 Runnable completionTask) {
			this.subscriber = subscriber;
			this.processorSubscription = processorSubscription;
			this.aeronHelper = aeronHelper;
			this.commandPub = commandPub;
			this.aliveSendersChecker = aliveSendersChecker;
			this.completionTask = completionTask;
		}

		@Override
		public void run() {
			try {
				subscriber.onSubscribe(processorSubscription);
			} catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				subscriber.onError(t);
			}

			incrementSubscribers();

			uk.co.real_logic.aeron.Subscription nextCompleteSub = aeronHelper.addSubscription(streamId);
			uk.co.real_logic.aeron.Subscription errorSub = aeronHelper.addSubscription(errorStreamId);

			final FragmentHandler errorFragmentAssembler = new FragmentAssembler(new ErrorFragmentHandler());
			final CompleteNextFragmentHandler completeNextFragmentHandler = new CompleteNextFragmentHandler();
			final FragmentAssembler completeNextFragmentAssembler = new FragmentAssembler(completeNextFragmentHandler);

			final IdleStrategy idleStrategy = AeronHelper.newBackoffIdleStrategy();
			final RequestCounter requestCounter = processorSubscription.getRequestCounter();
			try {
				while (processorSubscription.isActive() && !aliveSendersChecker.isAllDead()) {
					errorSub.poll(errorFragmentAssembler, 1);
					if (errorReceived) {
						break;
					}

					int fragmentLimit = (int) requestCounter.getNextRequestLimit();
					if (fragmentLimit == 0 && completeNextFragmentHandler.snappedNextMsg == null) {
						fragmentLimit = 1;
						completeNextFragmentHandler.shouldSnap = true;
					} else {
						completeNextFragmentHandler.shouldSnap = false;
					}

                    int nFragmentsReceived = 0;
                    if (fragmentLimit > 0) {
                        if (completeNextFragmentHandler.snappedNextMsg != null) {
                            subscriber.onNext(completeNextFragmentHandler.snappedNextMsg);
                            completeNextFragmentHandler.snappedNextMsg = null;
                            fragmentLimit--;
                            requestCounter.release(1);
                            nFragmentsReceived = 1;
                        }
                        if (fragmentLimit > 0) {
                            nFragmentsReceived += nextCompleteSub.poll(completeNextFragmentAssembler, fragmentLimit);
                            requestCounter.release(completeNextFragmentHandler.getAndClearNextEventsReceived());
                        }
					}
                    idleStrategy.idle(nFragmentsReceived);

					if (completeReceived) {
						completeReceived = false;

						aliveSendersChecker.scheduleCheck();
					}
				}
			} finally {
				nextCompleteSub.close();
				errorSub.close();

				if (aliveSendersChecker.isAllDead()) {
					// Executed when Complete was received and all publishers are dead

					subscriber.onComplete();
				} else if (!processorSubscription.isActive()) {
					// Executed when subscription was cancelled

					if (autoCancel) {
						sendCancelCommand();
					}
				}

				completionTask.run();
			}
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
	}

	/**
	 * Creates a new processor builder
	 *
	 * @return a processor builder
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Creates a new processor using the builder
	 *
	 * @param builder configuration of the processor
	 */
	AeronProcessor(Builder builder) {
		super(builder.name, builder.executorService, builder.autoCancel);
		this.channel = builder.channel;
		this.streamId = builder.streamId;
		this.errorStreamId = builder.errorStreamId;
		this.commandRequestStreamId = builder.commandRequestStreamId;
		this.commandReplyStreamId = builder.commandReplyStreamId;
		this.publicationLingerTimeoutMillis = builder.publicationLingerTimeoutMillis;
		this.publicationTimeoutMillis = builder.publicationTimeoutMillis;
		this.exceptionSerializer = new BasicExceptionSerializer();
		this.subscriber = new AeronProcessorSubscriber(builder.name, builder.ringBufferSize,
				builder.signalSenderContext, builder.launchEmbeddedMediaDriver, builder.multiPublishers);
		this.publisher = new AeronProcessorPublisher(builder.signalReceiverContext, builder.launchEmbeddedMediaDriver,
				builder.subscriberFragmentLimit, builder.cleanupDelayMillis);
	}

	/**
	 * Creates a new processor which supports publishing into itself
     * from a <b>single</b> thread.
	 *
	 * @param name processor's name used as a base name of subscriber threads
	 * @param autoCancel when set to true the processor will auto-cancel
	 * @param launchEmbeddedMediaDriver if embedded media driver should be launched
	 * @param channel Aeron channel used by the signals sender and the receiver
	 * @param streamId streamId for sending Next and Complete signals
	 * @param errorStreamId streamId for sending Error signals
	 * @param commandRequestStreamId streamId onto which signals sender
     *                               listens for commands from signals receiver
	 * @param commandReplyStreamId streamId onto which signals receiver
     *                             listens for command execution replies from
	 *                             signals sender
	 * @return a new processor
	 */
	public static AeronProcessor create(String name, boolean autoCancel, boolean launchEmbeddedMediaDriver,
										String channel,	int streamId, int errorStreamId, int commandRequestStreamId,
										int commandReplyStreamId) {
		return new Builder()
				.name(name)
				.autoCancel(autoCancel)
				.launchEmbeddedMediaDriver(launchEmbeddedMediaDriver)
				.channel(channel)
				.streamId(streamId)
				.errorStreamId(errorStreamId)
				.commandRequestStreamId(commandRequestStreamId)
				.commandReplyStreamId(commandReplyStreamId)
				.create();
	}

	/**
     * Creates a new processor which supports publishing into itself
     * from multiple threads.
	 *
	 * @param name processor's name used as a base name of subscriber threads
	 * @param autoCancel when set to true the processor will auto-cancel
	 * @param launchEmbeddedMediaDriver if embedded media driver should be launched
     * @param channel Aeron channel used by the signals sender and the receiver
     * @param streamId streamId for sending Next and Complete signals
     * @param errorStreamId streamId for sending Error signals
     * @param commandRequestStreamId streamId onto which signals sender
     *                               listens for commands from signals receiver
     * @param commandReplyStreamId streamId onto which signals receiver
     *                             listens for command execution replies from
     *                             signals sender
	 * @return a new processor
	 */
	public static AeronProcessor share(String name, boolean autoCancel, boolean launchEmbeddedMediaDriver,
									   String channel, int streamId, int errorStreamId, int commandRequestStreamId,
									   int commandReplyStreamId) {
		return new Builder()
				.name(name)
				.autoCancel(autoCancel)
				.launchEmbeddedMediaDriver(launchEmbeddedMediaDriver)
				.channel(channel)
				.streamId(streamId)
				.errorStreamId(errorStreamId)
				.commandRequestStreamId(commandRequestStreamId)
				.commandReplyStreamId(commandReplyStreamId)
				.share();
	}

	/**
	 * Returns available capacity for the number of messages which can be sent via the processor.
	 * Not implemented yet and always returns 0.
	 *
	 * @return 0
	 */
	@Override
	public long getAvailableCapacity() {
		return 0;
	}

	private class RingBufferProcessorSubscriber implements Subscriber<Buffer> {

		private final Publication nextCompletePub;

		private final BufferClaim bufferClaim;

		private final IdleStrategy idleStrategy;

		private final Publication errorPub;

		private final AeronHelper aeronHelper;

		RingBufferProcessorSubscriber(AeronHelper aeronHelper) {
			this.aeronHelper = aeronHelper;
			this.nextCompletePub = aeronHelper.addPublication(streamId);
			this.errorPub = aeronHelper.addPublication(errorStreamId);
			this.bufferClaim = new BufferClaim();
			this.idleStrategy = AeronHelper.newBackoffIdleStrategy();
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Buffer buffer) {
			if (publisher.alive.get()) {
				publishSignal(nextCompletePub, buffer, SignalType.Next, false);
			}
		}

		@Override
		public void onError(Throwable t) {
			Buffer buffer = Buffer.wrap(exceptionSerializer.serialize(t));
			if (publisher.alive.get()) {
				publishSignal(errorPub, buffer, SignalType.Error, true);
			}
			shutdown();
		}

		@Override
		public void onComplete() {
			Buffer buffer = new Buffer(0, true);
			if (publisher.alive.get()) {
				publishSignal(nextCompletePub, buffer, SignalType.Complete, true);
			}
			shutdown();
		}

		void shutdown() {
			subscriber.shutdown();

			// Covering the case when no subscribers are connected
			//   => no polling for Complete signal
			//     => There is nobody to shutdown Publisher
			if (checkNoSubscribersAttached()) {
				publisher.shutdown();
			}
		}

		void publishSignal(Publication publication, Buffer buffer, SignalType signalType, boolean waitLingerTimeout) {
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
			}
		}
	}

	class AeronProcessorSubscriber implements Subscriber<Buffer> {

		private final RingBufferProcessor<Buffer> processor;

		private final AeronHelper aeronHelper;

		private final CommandsPoller commandsPoller;

		private final AtomicBoolean alive = new AtomicBoolean(true);

		AeronProcessorSubscriber(String name, int ringBufferSize, Aeron.Context publisherCtx,
								 boolean launchEmbeddedMediaDriver, boolean multiPublishers) {
			if (multiPublishers) {
				this.processor = RingBufferProcessor.share(name + "-ring-buffer-consumer", ringBufferSize);
			} else {
				this.processor = RingBufferProcessor.create(name + "-ring-buffer-consumer", ringBufferSize);
			}

			this.aeronHelper = new AeronHelper(publisherCtx, launchEmbeddedMediaDriver,
					channel, publicationTimeoutMillis, publicationLingerTimeoutMillis);
			aeronHelper.initialise();

			processor.subscribe(new RingBufferProcessorSubscriber(aeronHelper));

			this.commandsPoller = new CommandsPoller(logger, aeronHelper, commandRequestStreamId, commandReplyStreamId);
			commandsPoller.initialize(executor);
		}

		@Override
		public void onSubscribe(Subscription s) {
			AeronProcessor.super.onSubscribe(s);
			commandsPoller.setUpstreamSubscription(upstreamSubscription);
		}

		@Override
		public void onNext(Buffer buffer) {
			if (buffer == null) {
				throw SpecificationExceptions.spec_2_13_exception();
			}
			processor.onNext(buffer);
		}

		@Override
		public void onError(Throwable t) {
			if (t == null) {
				throw new NullPointerException("Error could not be null");
			}
			commandsPoller.shutdown();
			processor.onError(t);
		}

		@Override
		public void onComplete() {
			commandsPoller.shutdown();
			processor.onComplete();
		}

		void shutdown() {
			if (alive.compareAndSet(true, false)) {
				aeronHelper.shutdown();
			}
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		subscriber.onSubscribe(s);
	}

	class AeronProcessorPublisher implements Publisher<Buffer> {

		private final int subscriberFragmentLimit;

		private final AeronHelper aeronHelper;

		private final Publication commandPub;

		private final AtomicBoolean alive = new AtomicBoolean(true);

		private final AliveSendersChecker aliveSendersChecker;

		AeronProcessorPublisher(Aeron.Context subscriberCtx, boolean launchEmbeddedMediaDriver,
								int subscriberFragmentLimit, int cleanupDelayMillis) {
			this.subscriberFragmentLimit = subscriberFragmentLimit;
			this.aeronHelper = new AeronHelper(subscriberCtx, launchEmbeddedMediaDriver,
					channel, publicationTimeoutMillis, publicationLingerTimeoutMillis);
			aeronHelper.initialise();
			this.commandPub = aeronHelper.addPublication(commandRequestStreamId);
			this.aliveSendersChecker = new AliveSendersChecker(logger, aeronHelper, commandPub,
					commandReplyStreamId, publicationLingerTimeoutMillis, cleanupDelayMillis);
		}

		@Override
		public void subscribe(Subscriber<? super Buffer> subscriber) {
			if (null == subscriber) {
				throw new NullPointerException("subscriber cannot be null");
			}

			AeronProcessorSubscription subscription = new AeronProcessorSubscription(subscriber,
					subscriberFragmentLimit, aeronHelper, commandPub);

			SignalsPoller signalsPoller = new SignalsPoller(subscriber, subscription, aeronHelper, commandPub,
					aliveSendersChecker, new Runnable() {
				@Override
				public void run() {
					// No more subscribers are attached => shutdown the processor
					if (SUBSCRIBER_COUNT.decrementAndGet(AeronProcessor.this) == 0) {
						shutdown();

						AeronProcessor.this.onComplete();
					}
				}
			});

			executor.execute(signalsPoller);
		}

		boolean shutdown() {
			if (!alive.compareAndSet(true, false)) {
				return false;
			}
			aliveSendersChecker.shutdown();
			commandPub.close();
			aeronHelper.shutdown();
			return true;
		}

	}

	protected boolean checkNoSubscribersAttached() {
		return SUBSCRIBER_COUNT.get(AeronProcessor.this) == 0;
	}

	@Override
	public void subscribe(Subscriber<? super Buffer> subscriber) {
		publisher.subscribe(subscriber);
	}

	@Override
	public boolean alive() {
		return !executor.isTerminated();
	}

	@Override
	public boolean isWork() {
		return false;
	}

	@Override
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		try {
			shutdown();
			return executor.awaitTermination(timeout, timeUnit);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	@Override
	public void forceShutdown() {
		shutdown();
	}

	/**
	 * Publishes Next signal containing <code>buffer</code> into Aeron.
	 *
	 * @param buffer buffer to be published
	 */
	@Override
	public void onNext(Buffer buffer) {
		subscriber.onNext(buffer);
	}

	@Override
	public void onError(Throwable t) {
        super.onError(t);
        subscriber.onError(t);
	}

	@Override
	public void onComplete() {
        super.onComplete();
        subscriber.onComplete();
	}

}
