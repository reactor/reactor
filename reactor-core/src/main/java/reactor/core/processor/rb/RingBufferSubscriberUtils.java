package reactor.core.processor.rb;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.ReactorProcessor;
import reactor.core.error.*;
import reactor.core.support.*;
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
			throw SpecificationExceptions.spec_2_13_exception();
		}

		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.type = MutableSignal.Type.NEXT;
		signal.value = value;
		signal.seqId = seqId;

		ringBuffer.publish(seqId);
	}

	public static <E> MutableSignal<E> prepareNext(RingBuffer<MutableSignal<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.seqId = seqId;
		return signal;
	}

	public static <E> void publish(RingBuffer<MutableSignal<E>> ringBuffer, MutableSignal<E> signal) {
		ringBuffer.publish(signal.seqId);
	}

	public static <E> void onError(Throwable error, RingBuffer<MutableSignal<E>> ringBuffer) {
		if (error == null) {
			throw SpecificationExceptions.spec_2_13_exception();
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
				} else {
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


	public static <E> Publisher<Void> writeWith(final Publisher<? extends E> source,
	                                            final RingBuffer<MutableSignal<E>> ringBuffer) {
		final Bounded nonBlockingSource = Bounded.class.isAssignableFrom(source.getClass()) ?
				(Bounded) source :
				null;

		final int capacity =
				nonBlockingSource != null ?
						(int) Math.min(nonBlockingSource.getCapacity(), ringBuffer.getBufferSize()) :
						ringBuffer.getBufferSize();

		return new WriteWithPublisher<>(source, ringBuffer, capacity);
	}

	private static class WriteWithPublisher<E> implements Publisher<Void> {
		private final Publisher<? extends E>       source;
		private final RingBuffer<MutableSignal<E>> ringBuffer;
		private final int                          capacity;

		public WriteWithPublisher(Publisher<? extends E> source, RingBuffer<MutableSignal<E>> ringBuffer, int capacity) {
			this.source = source;
			this.ringBuffer = ringBuffer;
			this.capacity = capacity;
		}

		@Override
		public void subscribe(final Subscriber<? super Void> s) {
			source.subscribe(new WriteWithSubscriber(s));
		}

		private class WriteWithSubscriber implements Subscriber<E> , Bounded {

			private final Sequence pendingRequest = new Sequence(0);
			private final Subscriber<? super Void> s;
			Subscription subscription;
			long         index;

			public WriteWithSubscriber(Subscriber<? super Void> s) {
				this.s = s;
				index = 0;
			}

			void doRequest(int n) {
				Subscription s = subscription;
				if (s != null) {
					pendingRequest.addAndGet(n);
					index = ringBuffer.next(n);
					s.request(n);
				}
			}

			@Override
			public void onSubscribe(Subscription s) {
				subscription = s;
				doRequest(capacity);
			}

			@Override
			public void onNext(E e) {
				long previous = pendingRequest.addAndGet(-1L);
				if (previous >= 0) {
					MutableSignal<E> signal = ringBuffer.get(index + (capacity - previous));
					signal.type = MutableSignal.Type.NEXT;
					signal.value = e;
					if (previous == 0 && subscription != null) {
						ringBuffer.publish(index - ((index + capacity) - 1), index);
						doRequest(capacity);
					}
				} else {
					throw reactor.core.error.InsufficientCapacityException.get();
				}
			}

			@Override
			public void onError(Throwable t) {
				s.onError(t);
			}

			@Override
			public void onComplete() {
				long previous = pendingRequest.get();
				ringBuffer.publish(index - ((index + (capacity)) - 1), (index - (capacity - previous)));
				s.onComplete();
			}

			@Override
			public boolean isReactivePull(ReactorProcessor dispatcher, long producerCapacity) {
				return false;
			}

			@Override
			public long getCapacity() {
				return capacity;
			}
		}
	}
}
