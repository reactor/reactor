package reactor.core.processor.util;

import org.reactivestreams.Subscriber;
import reactor.core.processor.MutableSignal;
import reactor.core.support.Exceptions;
import reactor.jarjar.com.lmax.disruptor.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Utility methods to perform common tasks associated with {@link org.reactivestreams.Subscriber} handling when the
 * signals are stored in a {@link com.lmax.disruptor.RingBuffer}.
 */
public final class RingBufferSubscriberUtils {

	private RingBufferSubscriberUtils() {
	}

	public static <E> void onNext(E value, RingBuffer<MutableSignal<E>> ringBuffer) {
		if (value == null) {
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}

		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.type = MutableSignal.Type.NEXT;
		signal.value = value;

		ringBuffer.publish(seqId);
	}

	public static <E> void onError(Throwable error, RingBuffer<MutableSignal<E>> ringBuffer) {
		if (error == null) {
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}

		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);

		signal.type = MutableSignal.Type.ERROR;
		signal.value = null;
		signal.error = error;

		ringBuffer.publish(seqId);
	}

	public static <E> void onComplete(RingBuffer<MutableSignal<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);

		signal.type = MutableSignal.Type.COMPLETE;
		signal.value = null;
		signal.error = null;

		ringBuffer.publish(seqId);
	}

	public static <E> void route(MutableSignal<E> task, Subscriber<? super E> subscriber) {
		if (task.type == MutableSignal.Type.NEXT && null != task.value) {
			// most likely case first
			subscriber.onNext(task.value);
		} else if (task.type == MutableSignal.Type.COMPLETE) {
			// second most likely case next
			subscriber.onComplete();
		} else if (task.type == MutableSignal.Type.ERROR) {
			// errors should be relatively infrequent compared to other signals
			subscriber.onError(task.error);
		}

	}


	public static <E> void routeOnce(MutableSignal<E> task, Subscriber<? super E> subscriber) {
		E value = task.value;
		task.value = null;
		try {
			if (task.type == MutableSignal.Type.NEXT && null != value) {
				// most likely case first
				subscriber.onNext(value);
			} else if (task.type == MutableSignal.Type.COMPLETE) {
				// second most likely case next
				subscriber.onComplete();
			} else if (task.type == MutableSignal.Type.ERROR) {
				// errors should be relatively infrequent compared to other signals
				subscriber.onError(task.error);
			}
		} catch (Throwable t) {
			task.value = value;
			throw t;
		}
	}

	public static <T> boolean waitRequestOrTerminalEvent(
			Sequence pendingRequest,
			RingBuffer<MutableSignal<T>> ringBuffer,
			SequenceBarrier barrier,
			Subscriber<? super T> subscriber,
			AtomicBoolean isRunning
	) {
		final long waitedSequence = ringBuffer.getCursor() + 1L;
		try {
			MutableSignal<T> event = null;
			while (pendingRequest.get() < 0l) {
				//pause until first request
				if (event == null) {
					barrier.waitFor(waitedSequence);
					event = ringBuffer.get(waitedSequence);

					if (event.type == MutableSignal.Type.COMPLETE) {
						try {
							subscriber.onComplete();
							return false;
						} catch (Throwable t) {
							Exceptions.throwIfFatal(t);
							subscriber.onError(t);
							return false;
						}
					} else if (event.type == MutableSignal.Type.ERROR) {
						subscriber.onError(event.error);
						return false;
					}
				}else{
					barrier.checkAlert();
				}
				LockSupport.parkNanos(1l);
			}
		} catch (TimeoutException te) {
			//ignore
		} catch (AlertException ae) {
			if (!isRunning.get()) {
				return false;
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}

		return true;
	}

}
