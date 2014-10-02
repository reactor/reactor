/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.event.dispatch.wait;

import reactor.jarjar.com.lmax.disruptor.*;

import java.util.concurrent.locks.LockSupport;

/**
 * @author Jon Brisbin
 */
public class ParkWaitStrategy implements WaitStrategy {

	private final long parkFor;

	public ParkWaitStrategy() {
		this(250);
	}

	public ParkWaitStrategy(long parkFor) {
		this.parkFor = parkFor;
	}

	@Override
	public long waitFor(long sequence,
	                    Sequence cursor,
	                    Sequence dependentSequence,
	                    SequenceBarrier barrier) throws AlertException,
	                                                    InterruptedException,
	                                                    TimeoutException {
		long availableSequence;
		while ((availableSequence = dependentSequence.get()) < sequence) {
			barrier.checkAlert();
			LockSupport.parkNanos(parkFor);
		}
		return availableSequence;
	}

	@Override
	public void signalAllWhenBlocking() {
	}

}
