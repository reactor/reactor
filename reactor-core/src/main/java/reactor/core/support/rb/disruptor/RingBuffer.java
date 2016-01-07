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

package reactor.core.support.rb.disruptor;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

import reactor.core.error.InsufficientCapacityException;
import reactor.core.support.WaitStrategy;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.LongSupplier;
import reactor.fn.Supplier;

/**
 * Ring based store of reusable entries containing the data representing an event being exchanged between event producer
 * and ringbuffer consumers.
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public abstract class RingBuffer<E> implements LongSupplier {

	public static final WaitStrategy.BusySpin NO_WAIT = new WaitStrategy.BusySpin();

	@SuppressWarnings("raw")
	public static final Supplier EMITTED = new Supplier() {
		@Override
		public Slot get() {
			return new Slot<>();
		}
	};

	/**
	 * A simple holder
	 * @param <T>
	 */
	public static final class Slot<T> {
		public T value = null;
	}

	/**
	 *
	 * @param buffer
	 * @param <T>
	 * @return
	 */
	public static  <T> Queue<T> newWriteQueue(RingBuffer<Slot<T>> buffer){
		return new WriteQueue<>(buffer);
	}

	/**
	 *
	 * @param buffer
	 * @param <T>
	 * @return
	 */
	public static  <T> Queue<T> newSequencedQueue(RingBuffer<Slot<T>> buffer){
		return newSequencedQueue(buffer, -1L);
	}

	/**
	 *
	 * @param buffer
	 * @param startSequence
	 * @param <T>
	 * @return
	 */
	public static <T> Queue<T> newSequencedQueue(RingBuffer<Slot<T>> buffer, long startSequence){
		return new SPSCQueue<>(buffer, startSequence);
	}

	/**
	 * Create a new multiple producer RingBuffer using the default wait strategy   {@link #NO_WAIT}.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @throws IllegalArgumentException if <tt>bufferSize</tt> is less than 1 or not a power of 2
	 * @see MultiProducerSequencer
	 */
	@SuppressWarnings("unchecked")
	public static <E> RingBuffer<Slot<E>> createMultiProducer(int bufferSize) {
		return createMultiProducer(EMITTED, bufferSize, new WaitStrategy.Blocking());
	}

	/**
	 * Create a new multiple producer RingBuffer using the default wait strategy   {@link #NO_WAIT}.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @throws IllegalArgumentException if <tt>bufferSize</tt> is less than 1 or not a power of 2
	 * @see MultiProducerSequencer
	 */
	public static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory, int bufferSize) {
		return createMultiProducer(factory, bufferSize, NO_WAIT);
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy  {@link #NO_WAIT}.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @see MultiProducerSequencer
	 */
	@SuppressWarnings("unchecked")
	public static <E> RingBuffer<Slot<E>> createSingleProducer(int bufferSize) {
		return createSingleProducer(EMITTED, bufferSize, NO_WAIT);
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy  {@link #NO_WAIT}.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @see MultiProducerSequencer
	 */
	@SuppressWarnings("unchecked")
	public static <E> RingBuffer<Slot<E>> createSingleProducer(int bufferSize, Runnable spinObserver) {
		return createSingleProducer(EMITTED, bufferSize, NO_WAIT, spinObserver);
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy   {@link #NO_WAIT}.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @see MultiProducerSequencer
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory, int bufferSize) {
		return createSingleProducer(factory, bufferSize, NO_WAIT);
	}

	/**
	 * Create a new multiple producer RingBuffer with the specified wait strategy.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 *
	 * @see MultiProducerSequencer
	 */
	public static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy) {
		return createMultiProducer(factory, bufferSize, waitStrategy, null);
	}

	/**
	 * Create a new multiple producer RingBuffer with the specified wait strategy.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 *
	 * @see MultiProducerSequencer
	 */
	public static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy,
			Runnable spinObserver) {

		if (PlatformDependent.hasUnsafe() && Sequencer.isPowerOfTwo(bufferSize)) {
			MultiProducerSequencer sequencer = new MultiProducerSequencer(bufferSize, waitStrategy, spinObserver);

			return new UnsafeRingBuffer<E>(factory, sequencer);
		}
		else {
			NotFunMultiProducerSequencer sequencer =
					new NotFunMultiProducerSequencer(bufferSize, waitStrategy, spinObserver);

			return new NotFunRingBuffer<E>(factory, sequencer);
		}
	}

	/**
	 * Create a new single producer RingBuffer with the specified wait strategy.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 *
	 * @see SingleProducerSequencer
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy) {
		return createSingleProducer(factory, bufferSize, waitStrategy, null);
	}

	/**
	 * Create a new single producer RingBuffer with the specified wait strategy.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @param spinObserver called each time the next claim is spinning and waiting for a slot
	 *
	 * @see SingleProducerSequencer
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy,
			Runnable spinObserver) {
		SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy, spinObserver);

		if (PlatformDependent.hasUnsafe() && Sequencer.isPowerOfTwo(bufferSize)) {
			return new UnsafeRingBuffer<>(factory, sequencer);
		}
		else {
			return new NotFunRingBuffer<>(factory, sequencer);
		}
	}

	/**
	 * <p>Get the event for a given sequence in the RingBuffer.</p>
	 *
	 * <p>This call has 2 uses.  Firstly use this call when publishing to a ring buffer. After calling {@link
	 * RingBuffer#next()} use this call to get hold of the preallocated event to fill with data before calling {@link
	 * RingBuffer#publish(long)}.</p>
	 *
	 * <p>Secondly use this call when consuming data from the ring buffer.  After calling {@link
	 * SequenceBarrier#waitFor(long)} call this method with any value greater than that your current consumer sequence
	 * and less than or equal to the value returned from the {@link SequenceBarrier#waitFor(long)} method.</p>
	 * @param sequence for the event
	 * @return the event for the given sequence
	 */
	abstract public E get(long sequence);

	/**
	 * Increment and return the next sequence for the ring buffer.  Calls of this method should ensure that they always
	 * publish the sequence afterward.  E.g.
	 * <pre>
	 * long sequence = ringBuffer.next();
	 * try {
	 *     Event e = ringBuffer.get(sequence);
	 *     // Do some work with the event.
	 * } finally {
	 *     ringBuffer.publish(sequence);
	 * }
	 * </pre>
	 * @return The next sequence to publish to.
	 * @see RingBuffer#publish(long)
	 * @see RingBuffer#get(long)
	 */
	abstract public long next();

	/**
	 * The same functionality as {@link RingBuffer#next()}, but allows the caller to claim the next n sequences.
	 * @param n number of slots to claim
	 * @return sequence number of the highest slot claimed
	 * @see Sequencer#next(int)
	 */
	abstract public long next(int n);

	/**
	 * <p>Increment and return the next sequence for the ring buffer.  Calls of this method should ensure that they
	 * always publish the sequence afterward.  E.g.
	 * <pre>
	 * long sequence = ringBuffer.next();
	 * try {
	 *     Event e = ringBuffer.get(sequence);
	 *     // Do some work with the event.
	 * } finally {
	 *     ringBuffer.publish(sequence);
	 * }
	 * </pre>
	 * <p>This method will not block if there is not space available in the ring buffer, instead it will throw an {@link
	 * InsufficientCapacityException}.
	 * @return The next sequence to publish to.
	 * @throws InsufficientCapacityException if the necessary space in the ring buffer is not available
	 * @see RingBuffer#publish(long)
	 * @see RingBuffer#get(long)
	 */
	abstract public long tryNext() throws InsufficientCapacityException;

	/**
	 * The same functionality as {@link RingBuffer#tryNext()}, but allows the caller to attempt to claim the next n
	 * sequences.
	 * @param n number of slots to claim
	 * @return sequence number of the highest slot claimed
	 * @throws InsufficientCapacityException if the necessary space in the ring buffer is not available
	 */
	abstract public long tryNext(int n) throws InsufficientCapacityException;

	/**
	 * Resets the cursor to a specific value.  This can be applied at any time, but it is worth noting that it can cause
	 * a data race and should only be used in controlled circumstances.  E.g. during initialisation.
	 * @param sequence The sequence to reset too.
	 * @throws IllegalStateException If any gating sequences have already been specified.
	 */
	abstract public void resetTo(long sequence);

	/**
	 * Sets the cursor to a specific sequence and returns the preallocated entry that is stored there.  This can cause a
	 * data race and should only be done in controlled circumstances, e.g. during initialisation.
	 * @param sequence The sequence to claim.
	 * @return The preallocated event.
	 */
	abstract public E claimAndGetPreallocated(long sequence);

	/**
	 * Determines if a particular entry has been published.
	 * @param sequence The sequence to identify the entry.
	 * @return If the value has been published or not.
	 */
	abstract public boolean isPublished(long sequence);

	/**
	 * Add the specified gating sequences to this instance of the Disruptor.  They will safely and atomically added to
	 * the list of gating sequences.
	 * @param gatingSequences The sequences to add.
	 */
	abstract public void addGatingSequences(Sequence... gatingSequences);

	/**
	 * Add the specified gating sequence to this instance of the Disruptor.  It will safely and atomically be added to
	 * the list of gating sequences and not RESET to the current ringbuffer cursor unlike addGatingSequences.
	 * @param gatingSequence The sequences to add.
	 */
	abstract public void addGatingSequence(Sequence gatingSequence);

	/**
	 * Get the minimum sequence value from all of the gating sequences added to this ringBuffer.
	 * @return The minimum gating sequence or the cursor sequence if no sequences have been added.
	 */
	abstract public long getMinimumGatingSequence();

	/**
	 * Get the minimum sequence value from all of the gating sequences added to this ringBuffer.
	 * @return The minimum gating sequence or the cursor sequence if no sequences have been added.
	 */
	abstract public long getMinimumGatingSequence(Sequence sequence);

	/**
	 * Remove the specified sequence from this ringBuffer.
	 * @param sequence to be removed.
	 * @return <tt>true</tt> if this sequence was found, <tt>false</tt> otherwise.
	 */
	abstract public boolean removeGatingSequence(Sequence sequence);

	/**
	 * Create a new SequenceBarrier to be used by an EventProcessor to track which messages are available to be read
	 * from the ring buffer given a list of sequences to track.
	 * @return A sequence barrier that will track the ringbuffer.
	 * @see SequenceBarrier
	 */
	abstract public SequenceBarrier newBarrier();

	/**
	 * Get the current cursor value for the ring buffer.  The actual value recieved will depend on the type of {@link
	 * Sequencer} that is being used.
	 * @see MultiProducerSequencer
	 * @see SingleProducerSequencer
	 */
	abstract public long getCursor();

	/**
	 * Get the current cursor value for the ring buffer.  The actual value recieved will depend on the type of {@link
	 * Sequencer} that is being used.
	 * @see MultiProducerSequencer
	 * @see SingleProducerSequencer
	 */
	abstract public Sequence getSequence();

	/**
	 * The size of the buffer.
	 */
	abstract public int getBufferSize();

	/**
	 * Given specified <tt>requiredCapacity</tt> determines if that amount of space is available.  Note, you can not
	 * assume that if this method returns <tt>true</tt> that a call to {@link RingBuffer#next()} will not block.
	 * Especially true if this ring buffer is set up to handle multiple producers.
	 * @param requiredCapacity The capacity to check for.
	 * @return <tt>true</tt> If the specified <tt>requiredCapacity</tt> is available <tt>false</tt> if now.
	 */
	abstract public boolean hasAvailableCapacity(int requiredCapacity);

	/**
	 * Publish the specified sequence.  This action marks this particular message as being available to be read.
	 * @param sequence the sequence to publish.
	 */
	abstract public void publish(long sequence);

	/**
	 * Publish the specified sequences.  This action marks these particular messages as being available to be read.
	 * @param lo the lowest sequence number to be published
	 * @param hi the highest sequence number to be published
	 * @see Sequencer#next(int)
	 */
	abstract public void publish(long lo, long hi);

	/**
	 * Get the remaining capacity for this ringBuffer.
	 * @return The number of slots remaining.
	 */
	abstract public long remainingCapacity();

	/**
	 * Get the pending capacity for this ringBuffer.
	 * @return The number of slots taken.
	 */
	abstract public long pending();

	/**
	 * Get the cached remaining capacity for this ringBuffer.
	 * @return The number of slots remaining.
	 */
	abstract public long cachedRemainingCapacity();

	abstract public Sequencer getSequencer();

	@Override
	public long get() {
		return getCursor();
	}

	static class WriteQueue<T> implements java.util.Queue<T>{
		final protected RingBuffer<Slot<T>> buffer;

		public WriteQueue(RingBuffer<Slot<T>> buffer) {
			this.buffer = buffer;
		}

		@Override
		public final boolean add(T o) {
			long seq = buffer.next();

			buffer.get(seq).value = o;
			buffer.publish(seq);
			return true;
		}

		@Override
		public final boolean offer(T o) {
			try {
				long seq = buffer.tryNext();

				buffer.get(seq).value = o;
				buffer.publish(seq);
				return true;
			}
			catch (InsufficientCapacityException ice){
				return false;
			}
		}

		@Override
		public  final boolean addAll(Collection<? extends T> c) {
			if(c.isEmpty()){
				return false;
			}
			for(T t : c){
				add(t);
			}
			return true;
		}

		@Override
		public boolean isEmpty() {
			return buffer.pending() == 0L;
		}

		@Override
		public int size() {
			return (int)buffer.pending();
		}

		@Override
		public T remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public T poll() {
			throw new UnsupportedOperationException();
		}

		@Override
		public T element() {
			throw new UnsupportedOperationException();
		}

		@Override
		public T peek() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean contains(Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<T> iterator() {
			throw new UnsupportedOperationException();
		}

		@Override
		@SuppressWarnings("unchecked")
		public T[] toArray() {
			throw new UnsupportedOperationException();
		}

		@Override
		@SuppressWarnings("unchecked")
		public <E> E[] toArray(E[] a) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String toString() {
			return "WriteQueue{" +
					"buffer=" + buffer +
					'}';
		}
	}

	static final class SPSCQueue<T>  extends WriteQueue<T> {


		final private Sequence pollCursor;

		SPSCQueue(RingBuffer<Slot<T>> buffer, long startingSequence) {
			super(buffer);
			this.pollCursor = Sequencer.newSequence(startingSequence);
			buffer.addGatingSequence(pollCursor);
		}

		@Override
		public T remove() {
			T e = poll();
			if(e == null){
				throw new NoSuchElementException();
			}
			return e;
		}

		@Override
		public T poll() {
			long current = buffer.getCursor();
			long cachedSequence = pollCursor.get() + 1L;

			if(cachedSequence <= current){
				T v = buffer.get(cachedSequence).value;
				if(v != null) {
					pollCursor.set(cachedSequence);
				}
				return v;
			}
			return null;
		}

		@Override
		public T element() {
			T e = peek();
			if(e == null){
				throw new NoSuchElementException();
			}
			return e;
		}

		@Override
		public T peek() {
			long current = buffer.getCursor();
			long cachedSequence = pollCursor.get() + 1L;

			if(cachedSequence <= current){
				return buffer.get(cachedSequence).value;
			}
			return null;
		}

		@Override
		public int size() {
			return (int)(buffer.getCursor() - pollCursor.get());
		}

		@Override
		public boolean isEmpty() {
			return buffer.getCursor() == pollCursor.get();
		}

		@Override
		public Iterator<T> iterator() {
			return new Iterator<T>() {

				@Override
				public boolean hasNext() {
					return !isEmpty();
				}

				@Override
				public T next() {
					return poll();
				}
			};
		}

		@Override
		@SuppressWarnings("unchecked")
		public T[] toArray() {
			return toArray((T[])new Object[buffer.getBufferSize()]);
		}

		@Override
		@SuppressWarnings("unchecked")
		public <E> E[] toArray(E[] a) {

			final long cursor = buffer.getCursor();
			long s = pollCursor.get() + 1L;
			final E[] array;
			final int n = (int)(cursor - s);

			if(n == 0){
				return a;
			}

			if(a.length < n){
				array = (E[])new Object[n];
			}
			else{
				array = a;
			}

			int i = 0;
			while(s < cursor){
				array[i++] = (E)buffer.get(cursor).value;
				s++;
			}
			return array;
		}

		@Override
		public void clear() {
			pollCursor.set(buffer.getCursor());
		}

		@Override
		public String toString() {
			return "SPSCQueue{" +
					"pollCursor=" + pollCursor +
					", parent=" + buffer.toString() +
					'}';
		}
	}

	@Override
	public String toString() {
		return "RingBuffer{pending:"+pending()+", size:"+getBufferSize()+", cursor:"+get()+", " +
				"min:"+getMinimumGatingSequence()+", subscribers:"+getSequencer().gatingSequences.length+"}";
	}
}
