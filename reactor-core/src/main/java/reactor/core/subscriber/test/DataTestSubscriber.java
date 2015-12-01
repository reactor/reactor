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

package reactor.core.subscriber.test;

import reactor.io.buffer.Buffer;

import java.util.ArrayList;
import java.util.List;

/**
 * Subscriber capturing Next signals for assertion
 *
 * @author Anatoly Kadyshev
 */
public class DataTestSubscriber extends TestSubscriber {

	/**
	 * Received Next signals
	 */
	private volatile List<String> nextSignals = new ArrayList<>();

	/**
	 * Creates a new test subscriber
	 *
	 * @param timeoutSecs timeout interval in seconds
	 * @return a newly created test subscriber
	 */
	public static DataTestSubscriber createWithTimeoutSecs(int timeoutSecs) {
		return new DataTestSubscriber(timeoutSecs);
	}

	private DataTestSubscriber(int timeoutSecs) {
		super(timeoutSecs);
	}

	/**
	 * Asserts that since the last call of the method Next signals
	 * {@code expectedNextSignals} were received in order.
	 *
	 * @param expectedNextSignals expected Next signals in order
	 *
	 * @throws InterruptedException if a thread was interrupted during a waiting
	 */
	public void assertNextSignals(String... expectedNextSignals) throws InterruptedException {
		int expectedNum = expectedNextSignals.length;
		assertNumNextSignalsReceived(expectedNum);

		List<String> nextSignalsSnapshot;
		synchronized (nextSignals) {
			nextSignalsSnapshot = nextSignals;
			nextSignals = new ArrayList<>();
		}

		if (nextSignalsSnapshot.size() != expectedNum) {
			throw new AssertionError(String.format("Expected %d number of signals but received %d",
					expectedNum,
					nextSignalsSnapshot.size()));
		}

		for (int i = 0; i < expectedNum; i++) {
			String expectedSignal = expectedNextSignals[i];
			String actualSignal = nextSignalsSnapshot.get(i);
			if (!actualSignal.equals(expectedSignal)) {
				throw new AssertionError(
						String.format("Expected Next signal: %s, but got: %s", expectedSignal, actualSignal));
			}
		}
	}

	@Override
	protected void doNext(Buffer buffer) {
		synchronized (nextSignals) {
			nextSignals.add(buffer.asString());
		}
		super.doNext(buffer);
	}

}
