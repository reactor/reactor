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
package reactor.core.processor.rb.disruptor;


import reactor.core.processor.rb.disruptor.util.Util;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

/**
 */
public final class RingBuffers
{
    /**
     * Create a new multiple producer RingBuffer with the specified wait strategy.
     *
     * @see MultiProducerSequencer
     * @param factory used to create the events within the ring buffer.
     * @param bufferSize number of elements to create within the ring buffer.
     * @param waitStrategy used to determine how to wait for new elements to become available.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     */
    public static <E> RingBuffer<E>  createMultiProducer(Supplier<E> factory,
                                                        int             bufferSize,
                                                        WaitStrategy    waitStrategy)
    {
        return createMultiProducer(factory, bufferSize, waitStrategy, null);
    }


    /**
     * Create a new multiple producer RingBuffer with the specified wait strategy.
     *
     * @see MultiProducerSequencer
     * @param factory used to create the events within the ring buffer.
     * @param bufferSize number of elements to create within the ring buffer.
     * @param waitStrategy used to determine how to wait for new elements to become available.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     */
    public static <E> RingBuffer<E>  createMultiProducer(Supplier<E> factory,
                                                        int             bufferSize,
                                                        WaitStrategy    waitStrategy,
                                                        Consumer<Void> spinObserver)
    {

        if(PlatformDependent.hasUnsafe() && Util.isPowerOfTwo(bufferSize)) {
            MultiProducerSequencer sequencer = new MultiProducerSequencer(bufferSize, waitStrategy, spinObserver);

            return new UnsafeRingBuffer<E>(factory, sequencer);
        }else{
            NotFunMultiProducerSequencer sequencer =
              new NotFunMultiProducerSequencer(bufferSize, waitStrategy, spinObserver);

            return new NotFunRingBuffer<E>(factory, sequencer);
        }
    }

    /**
     * Create a new multiple producer RingBuffer using the default wait strategy  {@link BlockingWaitStrategy}.
     *
     * @see MultiProducerSequencer
     * @param factory used to create the events within the ring buffer.
     * @param bufferSize number of elements to create within the ring buffer.
     * @throws IllegalArgumentException if <tt>bufferSize</tt> is less than 1 or not a power of 2
     */
    public static <E> RingBuffer<E>  createMultiProducer(Supplier<E> factory, int bufferSize)
    {
        return createMultiProducer(factory, bufferSize, new BlockingWaitStrategy());
    }

    /**
     * Create a new single producer RingBuffer with the specified wait strategy.
     *
     * @see SingleProducerSequencer
     * @param factory used to create the events within the ring buffer.
     * @param bufferSize number of elements to create within the ring buffer.
     * @param waitStrategy used to determine how to wait for new elements to become available.
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     */
    public static <E> RingBuffer<E>  createSingleProducer(Supplier<E> factory,
                                                         int             bufferSize,
                                                         WaitStrategy    waitStrategy)
    {
        return createSingleProducer(factory, bufferSize, waitStrategy, null);
    }


    /**
     * Create a new single producer RingBuffer with the specified wait strategy.
     *
     * @see SingleProducerSequencer
     * @param factory used to create the events within the ring buffer.
     * @param bufferSize number of elements to create within the ring buffer.
     * @param waitStrategy used to determine how to wait for new elements to become available.
     * @param spinObserver called each time the next claim is spinning and waiting for a slot
     * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
     */
    public static <E> RingBuffer<E>  createSingleProducer(Supplier<E> factory,
                                                         int             bufferSize,
                                                         WaitStrategy    waitStrategy,
                                                         Consumer<Void> spinObserver)
    {
        SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy, spinObserver);

        if(PlatformDependent.hasUnsafe() && Util.isPowerOfTwo(bufferSize)) {
            return new UnsafeRingBuffer<>(factory, sequencer);
        }else{
            return new NotFunRingBuffer<>(factory, sequencer);
        }
    }

    /**
     * Create a new single producer RingBuffer using the default wait strategy  {@link BlockingWaitStrategy}.
     *
     * @see MultiProducerSequencer
     * @param factory used to create the events within the ring buffer.
     * @param bufferSize number of elements to create within the ring buffer.
     * @throws IllegalArgumentException if <tt>bufferSize</tt> is less than 1 or not a power of 2
     */
    public static <E> RingBuffer<E>  createSingleProducer(Supplier<E> factory, int bufferSize)
    {
        return createSingleProducer(factory, bufferSize, new BlockingWaitStrategy());
    }

}
