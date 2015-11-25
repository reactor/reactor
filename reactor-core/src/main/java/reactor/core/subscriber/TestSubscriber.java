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

package reactor.core.subscriber;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Subscription;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;

/**
 * @author Anatoly Kadyshev
 * @since 2.1
 */
public class TestSubscriber extends SubscriberWithDemand<Buffer, Buffer> {

	/**
	 *
	 * @param timeoutSecs
	 * @return
	 */
	public static TestSubscriber createWithTimeoutSecs(int timeoutSecs) {
		return new TestSubscriber(timeoutSecs);
	}

	/**
	 *
	 * @param timeoutSecs
	 * @param errorMessageSupplier
	 * @param conditionSupplier
	 * @throws InterruptedException
	 */
	public static void waitFor(long timeoutSecs,
			Supplier<String> errorMessageSupplier,
			Supplier<Boolean> conditionSupplier) throws InterruptedException {
		Assert.notNull(errorMessageSupplier);
		Assert.notNull(conditionSupplier);
		Assert.isTrue(timeoutSecs > 0);

		long timeoutNs = TimeUnit.SECONDS.toNanos(timeoutSecs);
		long startTime = System.nanoTime();
		do {
			if (conditionSupplier.get()) {
				return;
			}
			Thread.sleep(100);
		}
		while (System.nanoTime() - startTime < timeoutNs);
		throw new AssertionError(errorMessageSupplier.get());
	}

	/**
	 *
	 * @param timeoutSecs
	 * @param errorMessage
	 * @param resultSupplier
	 * @throws InterruptedException
	 */
	public static void waitFor(long timeoutSecs, String errorMessage, Supplier<Boolean> resultSupplier)
			throws InterruptedException {
		waitFor(timeoutSecs, () -> errorMessage, resultSupplier);
	}

	//
	// Instance
	//

	private final AtomicInteger numNextSignalsReceived = new AtomicInteger(0);

	private final List<String> receivedSignals = new ArrayList<>();

	private final CountDownLatch completeLatch = new CountDownLatch(1);

	private final CountDownLatch errorLatch = new CountDownLatch(1);

	private final int timeoutSecs;

	private Throwable lastError;

	private TestSubscriber(int timeoutSecs) {
		super(null);
		this.timeoutSecs = timeoutSecs;
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public void requestUnboundedWithTimeout() throws InterruptedException {
		requestWithTimeout(Long.MAX_VALUE);
	}

	/**
	 *
	 * @param n
	 * @throws InterruptedException
	 */
	public void requestWithTimeout(long n) throws InterruptedException {
		waitFor(timeoutSecs,
				String.format("onSubscribe wasn't called within %d secs", timeoutSecs),
				() -> subscription != null);

		requestMore(n);
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public void sendUnboundedRequest() throws InterruptedException {
		requestMore(Long.MAX_VALUE);
	}

	/**
	 *
	 * @param n
	 */
	public void sendRequest(long n) {
		requestMore(n);
	}

	/**
	 *
	 * @param expectedSignals
	 * @throws InterruptedException
	 */
	public void assertNextSignals(String... expectedSignals) throws InterruptedException {
		assertNumNextSignalsReceived(expectedSignals.length);

		Set<String> signalsSnapshot;
		synchronized (receivedSignals) {
			signalsSnapshot = new HashSet<>(receivedSignals);
		}

		if (signalsSnapshot.size() != expectedSignals.length) {
			throw new AssertionError(String.format("Expected %d number of signals but received %d",
					expectedSignals.length,
					signalsSnapshot.size()));
		}

		for (String signal : expectedSignals) {
			signalsSnapshot.remove(signal);
		}

		if (signalsSnapshot.size() != 0) {
			throw new AssertionError("Unexpected signals received: " + signalsSnapshot);
		}
	}

	/**
	 *
	 * @param n
	 * @throws InterruptedException
	 */
	public void assertNumNextSignalsReceived(int n) throws InterruptedException {
		Supplier<String> errorSupplier = () -> String.format("%d out of %d Next signals received within %d secs",
				numNextSignalsReceived.get(),
				n,
				timeoutSecs);

		waitFor(timeoutSecs, errorSupplier, () -> numNextSignalsReceived.get() == n);
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public void assertCompleteReceived() throws InterruptedException {
		boolean result = completeLatch.await(timeoutSecs, TimeUnit.SECONDS);
		if (!result) {
			throw new AssertionError(String.format("Haven't received Complete signal within %d seconds", timeoutSecs));
		}
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public void assertNoCompleteReceived() throws InterruptedException {
		long startTime = System.nanoTime();
		do {
			if (completeLatch.getCount() == 0) {
				throw new AssertionError();
			}
			Thread.sleep(100);
		}
		while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime) < 1);
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public void assertErrorReceived() throws InterruptedException {
		boolean result = errorLatch.await(timeoutSecs, TimeUnit.SECONDS);
		if (!result) {
			throw new AssertionError(String.format("Haven't received Error signal within %d seconds", timeoutSecs));
		}
	}

	/**
	 *
	 * @return
	 */
	public Throwable getLastError() {
		return lastError;
	}

	/**
	 *
	 */
	public void cancelSubscription() {
		doCancel();
	}

	@Override
	protected void doOnSubscribe(Subscription s) {
		long toRequest = getRequested();
		if (toRequest > 0L) {
			s.request(toRequest);
		}
	}

	@Override
	protected void doNext(Buffer buffer) {
		BackpressureUtils.getAndSub(REQUESTED, this, 1L);
		numNextSignalsReceived.incrementAndGet();

		synchronized (receivedSignals) {
			receivedSignals.add(buffer.asString());
		}
	}

	@Override
	protected void doComplete() {
		completeLatch.countDown();
	}

	@Override
	protected void doError(Throwable t) {
		super.onError(t);
		this.lastError = t;
		errorLatch.countDown();
	}

}
