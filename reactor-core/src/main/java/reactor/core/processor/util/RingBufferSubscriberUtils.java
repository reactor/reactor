package reactor.core.processor.util;

import org.reactivestreams.Subscriber;
import reactor.core.processor.MutableSignal;
import reactor.jarjar.com.lmax.disruptor.RingBuffer;

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
		signal.error = null;
		//signal.consumed = false;

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
		//signal.consumed = false;

		ringBuffer.publish(seqId);
	}

	public static <E> void onComplete(RingBuffer<MutableSignal<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);

		signal.type = MutableSignal.Type.COMPLETE;
		signal.value = null;
		signal.error = null;
		//signal.consumed = false;

		ringBuffer.publish(seqId);
	}

	public static <E> void route(MutableSignal<E> task, Subscriber<? super E> subscriber) {
		//task.consumed = true;
		//try {
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
		//}catch(Throwable t){
			//task.consumed = false;
			//throw t;
		//}
	}

}
