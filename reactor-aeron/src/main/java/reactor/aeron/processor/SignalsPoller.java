package reactor.aeron.processor;

import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.error.Exceptions;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * Signals receiver functionality which polls for signals sent by senders
 */
class SignalsPoller implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SignalsPoller.class);

    private final Subscriber<? super Buffer> subscriber;

    private final AeronProcessorSubscription processorSubscription;

    private final AeronHelper aeronHelper;

    private final AliveSendersChecker aliveSendersChecker;

    private final int streamId;

    private final int errorStreamId;

    private final Runnable completionTask;

    private final Function<Void, Boolean> processorAliveFunction;

    private final Serializer<Throwable> exceptionSerializer;

    private final String senderChannel;

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
                         AeronHelper aeronHelper, String senderChannel,
                         AliveSendersChecker aliveSendersChecker,
                         Serializer<Throwable> exceptionSerializer,
                         int streamId,
                         int errorStreamId,
                         Runnable completionTask,
                         Function<Void, Boolean> processorAliveFunction) {
        this.subscriber = subscriber;
        this.processorSubscription = processorSubscription;
        this.aeronHelper = aeronHelper;
        this.aliveSendersChecker = aliveSendersChecker;
        this.exceptionSerializer = exceptionSerializer;
        this.senderChannel = senderChannel;
        this.streamId = streamId;
        this.errorStreamId = errorStreamId;
        this.completionTask = completionTask;
        this.processorAliveFunction = processorAliveFunction;
    }

    @Override
    public void run() {
        uk.co.real_logic.aeron.Subscription nextCompleteSub = aeronHelper.addSubscription(senderChannel, streamId);
        uk.co.real_logic.aeron.Subscription errorSub = aeronHelper.addSubscription(senderChannel, errorStreamId);

        //TODO: Possible timing issue due to CommandsPoller termination
        try {
            subscriber.onSubscribe(processorSubscription);
        } catch (Throwable t) {
            Exceptions.throwIfFatal(t);
            subscriber.onError(t);
        }

        final FragmentHandler errorFragmentAssembler = new FragmentAssembler(new ErrorFragmentHandler());
        final CompleteNextFragmentHandler completeNextFragmentHandler = new CompleteNextFragmentHandler();
        final FragmentAssembler completeNextFragmentAssembler = new FragmentAssembler(completeNextFragmentHandler);

        final IdleStrategy idleStrategy = AeronHelper.newBackoffIdleStrategy();
        final RequestCounter requestCounter = processorSubscription.getRequestCounter();
        try {
            while (processorSubscription.isActive()) {
                errorSub.poll(errorFragmentAssembler, 1);
                if (errorReceived) {
                    break;
                }

                if (aliveSendersChecker.isAllDead()) {
                    // Executed when Complete was received and all publishers are dead
                    subscriber.onComplete();
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
                    if (processorAliveFunction.apply(null)) {
                        completeReceived = false;
                        aliveSendersChecker.scheduleCheck();
                    } else {
                        subscriber.onComplete();
                        break;
                    }
                }
            }
        } finally {
            nextCompleteSub.close();
            errorSub.close();

            completionTask.run();
        }
    }

}
