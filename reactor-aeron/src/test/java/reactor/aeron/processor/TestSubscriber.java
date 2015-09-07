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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.io.buffer.Buffer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class TestSubscriber implements Subscriber<Buffer> {

    private final CountDownLatch eventsCountDownLatch;

    private final CountDownLatch completeLatch = new CountDownLatch(1);

    private final CountDownLatch errorLatch = new CountDownLatch(1);

    private final int timeoutSecs;

    private Throwable lastError;

    private Subscription subscription;

    public TestSubscriber(int timeoutSecs, int nExpectedEvents) {
        this.timeoutSecs = timeoutSecs;
        this.eventsCountDownLatch = new CountDownLatch(nExpectedEvents);
    }

    @Override
    public void onSubscribe(Subscription s) {
        this.subscription = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Buffer buffer) {
        eventsCountDownLatch.countDown();
    }

    @Override
    public void onError(Throwable t) {
        this.lastError = t;
        errorLatch.countDown();
    }

    public void cancel() {
        subscription.cancel();
    }

    @Override
    public void onComplete() {
        completeLatch.countDown();
    }

    public void assertAllEventsReceived() throws InterruptedException {
        boolean result = eventsCountDownLatch.await(timeoutSecs, TimeUnit.SECONDS);
        if (!result) {
            throw new AssertionError(
                    String.format("Didn't receive %d events within %d seconds",
                            eventsCountDownLatch.getCount(), timeoutSecs));
        }
    }

    public void assertCompleteReceived() throws InterruptedException {
        boolean result = completeLatch.await(timeoutSecs, TimeUnit.SECONDS);
        if (!result) {
            throw new AssertionError(
                    String.format("Haven't received Complete event within %d seconds", timeoutSecs));
        }
    }

    public void assertNoCompleteReceived() throws InterruptedException {
        long startTime = System.nanoTime();
        do {
            if (completeLatch.getCount() == 0) {
                throw new AssertionError("Unexpected Complete event received");
            }
            Thread.sleep(100);
        } while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime) < 1);
    }

    public void assertErrorReceived() throws InterruptedException {
        boolean result = errorLatch.await(timeoutSecs, TimeUnit.SECONDS);
        if (!result) {
            throw new AssertionError(
                    String.format("Haven't received Error event within %d seconds", timeoutSecs));
        }
    }

    public Throwable getLastError() {
        return lastError;
    }

}
