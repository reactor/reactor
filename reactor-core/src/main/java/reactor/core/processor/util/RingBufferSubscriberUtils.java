package reactor.core.processor.util;

import org.reactivestreams.Subscriber;
import reactor.core.processor.MutableSignal;
import reactor.jarjar.com.lmax.disruptor.RingBuffer;

/**
 * Created by jbrisbin on 3/24/15.
 */
public final class RingBufferSubscriberUtils {

	private RingBufferSubscriberUtils() {
	}

	public static <E> void onNext(E o, RingBuffer<MutableSignal<E>> ringBuffer) {
		if (o == null) {
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}

		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);

		signal.type = MutableSignal.Type.NEXT;
		signal.value = o;
		signal.error = null;

		ringBuffer.publish(seqId);
	}

	public static <E> void onError(Throwable t, RingBuffer<MutableSignal<E>> ringBuffer) {
		if (t == null) {
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}

		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);

		signal.type = MutableSignal.Type.ERROR;
		signal.value = null;
		signal.error = t;

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

	public static <E> void route(MutableSignal<E> task, Subscriber<? super E> sub) {
		try {
			if (task.type == MutableSignal.Type.NEXT && null != task.value) {
				// most likely case first
				sub.onNext(task.value);
			} else if (task.type == MutableSignal.Type.COMPLETE) {
				// second most likely case next
				sub.onComplete();
			} else if (task.type == MutableSignal.Type.ERROR) {
				// errors should be relatively infrequent compared to other signals
				sub.onError(task.error);
			}
		} catch (Throwable t) {
			sub.onError(t);
		}
	}

}
