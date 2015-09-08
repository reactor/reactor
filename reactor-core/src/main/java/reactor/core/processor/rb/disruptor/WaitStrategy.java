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


import reactor.fn.Consumer;
import reactor.fn.LongSupplier;

/**
 * Strategy employed for making ringbuffer consumers wait on a cursor {@link Sequence}.
 */
public interface WaitStrategy
{
    /**
     * Wait for the given sequence to be available.  It is possible for this method to return a value
     * less than the sequence number supplied depending on the implementation of the WaitStrategy.  A common
     * use for this is to signal a timeout.  Any EventProcessor that is using a WaitStragegy to get notifications
     * about message becoming available should remember to handle this case.
     *
     * @param sequence to be waited on.
     * @param cursor the main sequence from ringbuffer. Wait/notify strategies will
     *    need this as is notified upon update.
     * @param spinObserver Spin observer
     * @return the sequence that is available which may be greater than the requested sequence.
     * @throws AlertException if the status of the Disruptor has changed.
     * @throws InterruptedException if the thread is interrupted.
     */
    long waitFor(long sequence, LongSupplier cursor, Consumer<Void> spinObserver)
        throws AlertException, InterruptedException;

    /**
     * Implementations should signal the waiting ringbuffer consumers that the cursor has advanced.
     */
    void signalAllWhenBlocking();
}
