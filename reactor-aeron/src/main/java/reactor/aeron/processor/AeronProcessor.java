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
import org.slf4j.LoggerFactory;
import reactor.core.processor.ExecutorPoweredProcessor;
import reactor.core.support.Exceptions;
import reactor.core.support.SpecificationExceptions;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * A processor which can publish into and subscribe to data from Aeron.
 *
 * <ul>
 *     <li>When auto-cancel is enabled and the last subscriber is unregistered
 *         a cancel event is sent into upstream Publisher if any.</li>
 *     <li>When no subscribers are connected and too many messages were published so that the Aeron buffer is full
 *         method{@link #onNext(Buffer)} will block until messages are read</li>
 * </ul>
 *
 * @author Anatoly Kadyshev
 */
public class AeronProcessor extends ExecutorPoweredProcessor<Buffer, Buffer> {

    private static final Logger logger = LoggerFactory.getLogger(AeronProcessor.class);

    private static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

    private final String channel;

    private final int streamId;

    private final AeronPublisher aeronPublisher;

    private final AeronSubscriber aeronSubscriber;

    private final MediaDriver driver;

    private volatile boolean shutdownInProgress;

    enum SignalType {

        Next((byte)0),
        Error((byte)1),
        Complete((byte)2);

        private final byte code;

        SignalType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

    }

    class AeronProcessorSubscription implements Subscription {

        private final Subscriber<? super Buffer> subscriber;

        private volatile boolean active = true;

        //TODO: Make configurable
        private static final int DEFAULT_FRAGMENT_LIMIT = 42;

        private final RequestCounter requestCounter = new RequestCounter(DEFAULT_FRAGMENT_LIMIT);

        public AeronProcessorSubscription(Subscriber<? super Buffer> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (n <= 0l) {
                subscriber.onError(SpecificationExceptions.spec_3_09_exception(n));
                return;
            }

            if (upstreamSubscription != null) {
                upstreamSubscription.request(n);
            }

            requestCounter.request(n);
        }

        @Override
        public void cancel() {
            active = false;
        }

    }

    interface AeronPublisher {

        void initialise();

        void shutdown();

        void publish(Buffer buffer, SignalType signalType);

    }

    private abstract class BaseAeronPublisher implements AeronPublisher {

        private Aeron aeron;

        protected Publication publication;

        private final Aeron.Context ctx;

        public BaseAeronPublisher(Aeron.Context ctx) {
            this.ctx = ctx;
        }

        @Override
        public void initialise() {
            this.aeron = Aeron.connect(ctx);
            this.publication = aeron.addPublication(channel, streamId);
        }

        protected void doPublish(Buffer buffer, BufferClaim bufferClaim, IdleStrategy idleStrategy, SignalType signalType) {
            long result;
            int limit = buffer.limit();
            while((result = publication.tryClaim(limit + 1, bufferClaim)) < 0){
                if (result != Publication.BACK_PRESSURED && result != Publication.NOT_CONNECTED) {
                    throw new RuntimeException("Could not publish into Aeron because of an unknown reason");
                }
                idleStrategy.idle(0);
            }

            try {
                MutableDirectBuffer mutableBuffer = bufferClaim.buffer();
                int offset = bufferClaim.offset();
                mutableBuffer.putByte(offset, signalType.code);
                mutableBuffer.putBytes(offset + 1, buffer.byteBuffer(), limit);
            } finally {
                bufferClaim.commit();
            }
        }

        @Override
        public void shutdown() {
            publication.close();
            aeron.close();
        }

    }

    private class SingleThreadedAeronPublisher extends BaseAeronPublisher {

        private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        private final BufferClaim bufferClaim = new BufferClaim();

        public SingleThreadedAeronPublisher(Aeron.Context ctx) {
            super(ctx);
        }

        @Override
        public void publish(Buffer buffer, SignalType signalType) {
            doPublish(buffer, bufferClaim, idleStrategy, signalType);
        }

    }

    class MultiThreadedAeronPublisher extends BaseAeronPublisher {

        public MultiThreadedAeronPublisher(Aeron.Context ctx) {
            super(ctx);
        }

        @Override
        public void publish(Buffer buffer, SignalType signalType) {
            IdleStrategy idleStrategy = new BusySpinIdleStrategy();
            BufferClaim bufferClaim = new BufferClaim();
            doPublish(buffer, bufferClaim, idleStrategy, signalType);
        }

    }

    class AeronSubscriber {

        private Aeron aeron;

        private final Aeron.Context ctx;

        public AeronSubscriber(Aeron.Context ctx) {
            this.ctx = ctx;
        }

        void initialise() {
            this.aeron = Aeron.connect(ctx);
        }

        public uk.co.real_logic.aeron.Subscription addNewSubscription() {
            return aeron.addSubscription(channel, streamId);
        }

        public void shutdown() {
            aeron.close();
        }

    }

    private class AeronSubscriberPoller implements Runnable {

        private final Subscriber<? super Buffer> subscriber;

        private final AeronProcessorSubscription processorSubscription;

        private volatile boolean onCompleteReceived = false;

        public AeronSubscriberPoller(Subscriber<? super Buffer> subscriber,
                                     AeronProcessorSubscription processorSubscription) {
            this.subscriber = subscriber;
            this.processorSubscription = processorSubscription;
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

            uk.co.real_logic.aeron.Subscription aeronSubscription = aeronSubscriber.addNewSubscription();

            final FragmentHandler fragmentHandler = new FragmentAssembler(new FragmentHandler() {

                @Override
                public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                    byte[] data = new byte[length - 1];
                    buffer.getBytes(offset + 1, data);
                    byte signalTypeCode = buffer.getByte(offset);
                    try {
                        handleSignal(signalTypeCode, data);
                    } catch (Throwable t) {
                        Exceptions.throwIfFatal(t);
                        subscriber.onError(t);
                    }
                }

                private void handleSignal(byte signalTypeCode, byte[] data) {
                    if (signalTypeCode == SignalType.Next.getCode()) {

                        Buffer wrapper = Buffer.wrap(data);
                        subscriber.onNext(wrapper);

                    } else if (signalTypeCode == SignalType.Error.getCode()) {

                        String errorMessage = new String(data, UTF_8_CHARSET);
                        subscriber.onError(new Exception("Received an error from the server with message: " +
                                errorMessage));

                    } else if (signalTypeCode == SignalType.Complete.getCode()) {

                        try {
                            subscriber.onComplete();
                        } finally {
                            onCompleteReceived = true;
                        }

                    } else {
                        logger.error("Message with unknown status code of {} and length of {} ignored",
                                signalTypeCode, data.length);
                    }
                }

            });

            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                    100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

            try {
                while (processorSubscription.active && !onCompleteReceived && !shutdownInProgress) {
                    final int fragmentLimit = (int) processorSubscription.requestCounter.getNextRequestLimit();
                    int fragmentsReceived = 0;
                    if (fragmentLimit > 0) {
                        fragmentsReceived = aeronSubscription.poll(fragmentHandler, fragmentLimit);
                        processorSubscription.requestCounter.release(fragmentsReceived);
                    }
                    idleStrategy.idle(fragmentsReceived);
                }
            } finally {
                aeronSubscription.close();
                if (decrementSubscribers() == 0 && onCompleteReceived) {
                    shutdown();
                }
            }
        }

    }

    /**
     * Creates a new processor with name <code>name</code> which expects publishing into itself from a single thread
     * on channel <code>channel</code> and stream <code>stream</code>
     *
     * @param name processor's name used as a base name of subscriber threads
     * @param autoCancel when set to true the processor will auto-cancel
     * @param useEmbeddedMediaDriver if embedded media driver should be used
     * @param channel onto which publishing and subscribing should be done
     * @param streamId onto which publishing and subscribing should be done for provided <code>channel</code>
     * @return a new processor
     */
    public static AeronProcessor create(String name, boolean autoCancel, boolean useEmbeddedMediaDriver,
                                        String channel, int streamId) {
        return new AeronProcessor(name, autoCancel, useEmbeddedMediaDriver, channel, streamId, false);
    }

    /**
     * Creates a processor with name <code>name</code> into which publishing can be done from multiple threads
     * on channel <code>channel</code> and stream <code>stream</code>
     *
     * @param name processor's name used as a base name of subscriber threads
     * @param autoCancel when set to true the processor will auto-cancel
     * @param useEmbeddedMediaDriver if embedded media driver should be used
     * @param channel onto which publishing and subscribing should be done
     * @param streamId onto which publishing and subscribing should be done for provided <code>channel</code>
     * @return a new processor
     */
    public static AeronProcessor share(String name, boolean autoCancel, boolean useEmbeddedMediaDriver,
                                       String channel, int streamId) {
        return new AeronProcessor(name, autoCancel, useEmbeddedMediaDriver, channel, streamId, true);
    }

    private AeronProcessor(String name, boolean autoCancel, boolean useEmbeddedMediaDriver, String channel,
                           int streamId, boolean multiPublishers) {
        super(name, null, autoCancel);
        this.channel = channel;
        this.streamId = streamId;
        this.driver = useEmbeddedMediaDriver ? MediaDriver.launchEmbedded() : null;

        this.aeronPublisher = createAeronPublisher(multiPublishers, createDefaultAeronContext(useEmbeddedMediaDriver));
        this.aeronSubscriber = createAeronSubscriber(createDefaultAeronContext(useEmbeddedMediaDriver));
    }

    private Aeron.Context createDefaultAeronContext(boolean useEmbeddedMediaDriver) {
        Aeron.Context ctx = new Aeron.Context();
        if (useEmbeddedMediaDriver) {
            ctx.dirName(driver.contextDirName());
        }
        return ctx;
    }

    private AeronSubscriber createAeronSubscriber(Aeron.Context ctx) {
        AeronSubscriber subscriber = new AeronSubscriber(ctx);
        subscriber.initialise();
        return subscriber;
    }

    private AeronPublisher createAeronPublisher(boolean multiPublishers, Aeron.Context ctx) {
        AeronPublisher publisher;
        if (multiPublishers) {
            publisher = new MultiThreadedAeronPublisher(ctx);
        } else {
            publisher = new SingleThreadedAeronPublisher(ctx);
        }
        publisher.initialise();
        return publisher;
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

    @Override
    public void subscribe(Subscriber<? super Buffer> subscriber) {
        AeronProcessorSubscription subscription = new AeronProcessorSubscription(subscriber);
        executor.execute(new AeronSubscriberPoller(subscriber, subscription));
    }

    @Override
    public boolean alive() {
        return !executor.isTerminated();
    }

    @Override
    public void shutdown() {
        shutdownInProgress = true;

        aeronPublisher.shutdown();

        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        while (SUBSCRIBER_COUNT.get(this) > 0) {
            idleStrategy.idle(0);
        }

        aeronSubscriber.shutdown();
        CloseHelper.quietClose(driver);
        super.onComplete();
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
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void forceShutdown() {
        shutdown();
    }

    /**
     * Buffer to be published into Aeron the first byte of which should be 0 for signal type passing.
     *
     * @param buffer buffer the first byte of which is used for signal type and should always be 0
     * @throws IllegalArgumentException if the first byte of the buffer is not 0
     */
    @Override
    public void onNext(Buffer buffer) {
        if (buffer == null) {
            throw SpecificationExceptions.spec_2_13_exception();
        }

        aeronPublisher.publish(buffer, SignalType.Next);
    }

    @Override
    public void onError(Throwable t) {
        String errorMessage = t.getMessage();
        if (errorMessage == null) {
            errorMessage = "";
        }
        Buffer buffer = Buffer.wrap(errorMessage.getBytes(UTF_8_CHARSET));
        aeronPublisher.publish(buffer, SignalType.Error);
    }

    @Override
    public void onComplete() {
        Buffer buffer = new Buffer(0, true);
        aeronPublisher.publish(buffer, SignalType.Complete);
    }

}
