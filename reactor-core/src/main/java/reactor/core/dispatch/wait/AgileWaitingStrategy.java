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
package reactor.core.dispatch.wait;

import reactor.jarjar.com.lmax.disruptor.*;

/**
 * A pair of slow and fast wait strategies to dynamically adapt to a given application load
 *
* @author Stephane Maldini
 *
 * @since 2.0
*/
public final class AgileWaitingStrategy implements WaitStrategy, WaitingMood {

	private final WaitStrategy slowWaitStrategy;
	private final WaitStrategy fastWaitStrategy;

	private WaitStrategy currentStrategy;

	public AgileWaitingStrategy(){
		this(new BlockingWaitStrategy(), new YieldingWaitStrategy());
	}

	public AgileWaitingStrategy(WaitStrategy slowWaitStrategy, WaitStrategy fastWaitStrategy) {
		this.slowWaitStrategy = slowWaitStrategy;
		this.fastWaitStrategy = fastWaitStrategy;
		this.currentStrategy = slowWaitStrategy;
	}

	@Override
	public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier)
			throws AlertException, InterruptedException, TimeoutException {
		return currentStrategy.waitFor(sequence, cursor, dependentSequence, barrier);
	}

	@Override
	public void signalAllWhenBlocking() {
		currentStrategy.signalAllWhenBlocking();
	}

	@Override
	public void nervous(){
		currentStrategy = fastWaitStrategy;
		slowWaitStrategy.signalAllWhenBlocking();
	}

	@Override
	public void calm(){
		currentStrategy = slowWaitStrategy;
		fastWaitStrategy.signalAllWhenBlocking();
	}

	public WaitStrategy current(){
		return currentStrategy;
	}
}
