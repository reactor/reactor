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
package reactor.aeron.processor;

import org.reactivestreams.Subscription;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Anatoly Kadyshev
 */
public class StepByStepTestSubscriber extends BaseSubscriber<Buffer> {

	private volatile Subscription subscription;

	private final AtomicInteger numNextSignalsReceived = new AtomicInteger(0);

	private final List<String> dataList = new ArrayList<>();

	private final CountDownLatch completeLatch = new CountDownLatch(1);

	private final CountDownLatch errorLatch = new CountDownLatch(1);

	private final int timeoutSecs;

	public StepByStepTestSubscriber(int timeoutSecs) {
		this.timeoutSecs = timeoutSecs;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if(BackpressureUtils.checkSubscription(subscription, s)) {
			this.subscription = s;
		}
	}

	@Override
	public void onNext(Buffer buffer) {
		super.onNext(buffer);

		numNextSignalsReceived.incrementAndGet();

		synchronized (dataList) {
			dataList.add(buffer.asString());
		}
	}

	@Override
	public void onComplete() {
		completeLatch.countDown();
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);
		errorLatch.countDown();
	}

	public void request(int n) throws InterruptedException {
		TestUtils.waitForTrue(timeoutSecs,
				String.format("onSubscribe wasn't called within %d secs", timeoutSecs),
				() -> subscription != null);

		subscription.request(n);
	}

	public void assertNextSignals(String... expectedData) throws InterruptedException {
		assertNumNextSignalsReceived(expectedData.length);

		Set<String> actualData;
		synchronized (dataList) {
			actualData = new HashSet<>(dataList);
		}

		if (actualData.size() != expectedData.length) {
			throw new AssertionError(String.format("Expected %d data items but received %d",
					expectedData.length, actualData.size()));
		}

		for (String signal : expectedData) {
			actualData.remove(signal);
		}

		if (actualData.size() != 0) {
			throw new AssertionError("Unexpected data items received: " + actualData);
		}
	}

	public void assertNumNextSignalsReceived(int n) throws InterruptedException {
		Supplier<String> errorSupplier = () -> String.format("%d out of %d Next signals received within %d secs",
				numNextSignalsReceived.get(), n, timeoutSecs);

		TestUtils.waitForTrue(timeoutSecs, errorSupplier, () -> numNextSignalsReceived.get() == n);

		Thread.sleep(250);

		if (numNextSignalsReceived.get() != n) {
			throw new AssertionError(errorSupplier.get());
		}
	}

	public void assertCompleteReceived() throws InterruptedException {
		boolean result = completeLatch.await(timeoutSecs, TimeUnit.SECONDS);
		if (!result) {
			throw new AssertionError(
					String.format("Haven't received Complete event within %d seconds", timeoutSecs));
		}
	}

	public void assertErrorReceived() throws InterruptedException {
		boolean result = errorLatch.await(timeoutSecs, TimeUnit.SECONDS);
		if (!result) {
			throw new AssertionError(
					String.format("Haven't received Error event within %d seconds", timeoutSecs));
		}
	}

}
