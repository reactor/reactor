package reactor.aeron.processor;

import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.error.Exceptions;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Subscription;
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

	private final Runnable completionTask;

	private final Function<Void, Boolean> processorAliveFunction;

	private final Serializer<Throwable> exceptionSerializer;

	private final Context context;

	private final ErrorFragmentHandler errorFragmentHandler;

	private final CompleteNextFragmentHandler completeNextFragmentHandler;

	private abstract class SignalsPollerFragmentHandler implements FragmentHandler {

		private final FragmentAssembler fragmentAssembler = new FragmentAssembler(new FragmentHandler() {
			@Override
			public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
				doOnFragment(buffer, offset, length, header);
			}
		});

		@Override
		public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
			fragmentAssembler.onFragment(buffer, offset, length, header);
		}

		private void doOnFragment(DirectBuffer buffer, int offset, int length, Header header) {
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
		boolean checkForComplete;

		/**
		 * Message read from Aeron but not yet forwarded into a subscriber
		 */
		Buffer reservedNextSignal;

		/**
		 * Number of Next signals received
		 */
		int nNextSignalsReceived;

		/**
		 * Complete signal was received from one of senders
		 */
		private boolean completeReceived = false;

		@Override
		boolean handleSignal(byte signalTypeCode, byte[] data, int sessionId) {
			if (signalTypeCode == SignalType.Next.getCode()) {
				Buffer buffer = Buffer.wrap(data);
				if (checkForComplete) {
					reservedNextSignal = buffer;
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

		public boolean hasReservedNextSignal() {
			return reservedNextSignal != null;
		}

		public void processReservedNextSignal() {
			subscriber.onNext(reservedNextSignal);
			reservedNextSignal = null;
		}

		public boolean getAndResetCompleteReceived() {
			boolean result = completeReceived;
			completeReceived = false;
			return result;
		}
	}

	/**
	 * Handler for Error signals
	 */
	private class ErrorFragmentHandler extends SignalsPollerFragmentHandler {

		/**
		 * Error signal was received from one of senders
		 */
		private boolean errorReceived = false;

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

		boolean isErrorReceived() {
			return errorReceived;
		}

	}

	public SignalsPoller(Context context,
						 Subscriber<? super Buffer> subscriber,
						 AeronProcessorSubscription processorSubscription,
						 AeronHelper aeronHelper,
						 AliveSendersChecker aliveSendersChecker,
						 Serializer<Throwable> exceptionSerializer,
						 Runnable completionTask,
						 Function<Void, Boolean> processorAliveFunction) {
		this.context = context;
		this.subscriber = subscriber;
		this.processorSubscription = processorSubscription;
		this.aeronHelper = aeronHelper;
		this.aliveSendersChecker = aliveSendersChecker;
		this.exceptionSerializer = exceptionSerializer;
		this.completionTask = completionTask;
		this.processorAliveFunction = processorAliveFunction;
		this.errorFragmentHandler = new ErrorFragmentHandler();
		this.completeNextFragmentHandler = new CompleteNextFragmentHandler();
	}

	@Override
	public void run() {
		uk.co.real_logic.aeron.Subscription nextCompleteSub = createNextCompleteSub();
		uk.co.real_logic.aeron.Subscription errorSub = createErrorSub();

		passSubscriptionToSubscriber();

		final IdleStrategy idleStrategy = AeronHelper.newBackoffIdleStrategy();
		final RequestCounter requestCounter = processorSubscription.getRequestCounter();
		try {
			while (processorSubscription.isActive()) {
				errorSub.poll(errorFragmentHandler, 1);
				if (errorFragmentHandler.isErrorReceived()) {
					break;
				}

				if (aliveSendersChecker.isNoSendersDetected()) {
					subscriber.onComplete();
					break;
				}

				int nFragmentsReceived = 0;
				int fragmentLimit = (int) requestCounter.getNextRequestLimit();
				if (fragmentLimit == 0) {
					if (!completeNextFragmentHandler.hasReservedNextSignal()) {
						checkForCompleteSignal(nextCompleteSub);
					}
				} else {
					if (completeNextFragmentHandler.hasReservedNextSignal()) {
						completeNextFragmentHandler.processReservedNextSignal();
						fragmentLimit--;
						requestCounter.release(1);
					}
					if (fragmentLimit > 0) {
						nFragmentsReceived = nextCompleteSub.poll(completeNextFragmentHandler, fragmentLimit);
						requestCounter.release(completeNextFragmentHandler.getAndClearNextEventsReceived());
					}
				}
				idleStrategy.idle(nFragmentsReceived);

				if (completeNextFragmentHandler.getAndResetCompleteReceived()) {
					if (processorAliveFunction.apply(null)) {
						aliveSendersChecker.scheduleCheckForAliveSenders();
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

	private void passSubscriptionToSubscriber() {
		//TODO: Possible timing issue due to CommandsPoller termination
		try {
			subscriber.onSubscribe(processorSubscription);
		} catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			subscriber.onError(t);
		}
	}

	private Subscription createErrorSub() {
		return aeronHelper.addSubscription(context.receiverChannel, context.errorStreamId);
	}

	private Subscription createNextCompleteSub() {
		return aeronHelper.addSubscription(context.receiverChannel, context.streamId);
	}

	private void checkForCompleteSignal(Subscription nextCompleteSub) {
		completeNextFragmentHandler.checkForComplete = true;
		nextCompleteSub.poll(completeNextFragmentHandler, 1);
		completeNextFragmentHandler.checkForComplete = false;
	}

}
