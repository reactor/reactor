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

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.io.buffer.Buffer;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Streams;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronProcessorTest {

    private String CHANNEL = "udp://localhost:" + SocketUtils.findAvailableUdpPort();

    private int STREAM_ID = 10;

    private AeronProcessor processor;

    private static class SubscriberForTest implements Subscriber<Buffer> {

        private static final long TIMEOUT_SECS = 5;

        private final CountDownLatch eventsCountDownLatch;

        private final CountDownLatch completedLatch = new CountDownLatch(1);

        private final CountDownLatch errorLatch = new CountDownLatch(1);

        private Throwable lastError;

        public SubscriberForTest(int nExpectedEvents) {
            this.eventsCountDownLatch = new CountDownLatch(nExpectedEvents);
        }

        @Override
        public void onSubscribe(Subscription s) {
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

        @Override
        public void onComplete() {
            completedLatch.countDown();
        }

        public boolean awaitAllEvents() throws InterruptedException {
            return eventsCountDownLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS);
        }

        public boolean awaitCompleted() throws InterruptedException {
            return completedLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS);
        }

        public boolean awaitError() throws InterruptedException {
            return errorLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS);
        }

        public Throwable getLastError() {
            return lastError;
        }
    }

    @Before
    public void doSetup() {
        processor = AeronProcessor.create("aeron", false, true, CHANNEL, STREAM_ID);
    }

    @After
    public void doTearDown() throws InterruptedException {
        processor.shutdown();
    }

    @Test
    public void testOnNext() throws InterruptedException {
        SubscriberForTest subscriber = new SubscriberForTest(4);
        processor.subscribe(subscriber);

        processor.onNext(Buffer.wrap("Live"));
        processor.onNext(Buffer.wrap("Hard"));
        processor.onNext(Buffer.wrap("Die"));
        processor.onNext(Buffer.wrap("Harder"));

        assertTrue(subscriber.awaitAllEvents());
    }

    @Test
    public void testWorksWithTwoSubscribers() throws InterruptedException {
        SubscriberForTest subscriber1 = new SubscriberForTest(4);
        processor.subscribe(subscriber1);
        SubscriberForTest subscriber2 = new SubscriberForTest(4);
        processor.subscribe(subscriber2);

        processor.onNext(Buffer.wrap("Live"));
        processor.onNext(Buffer.wrap("Hard"));
        processor.onNext(Buffer.wrap("Die"));
        processor.onNext(Buffer.wrap("Harder"));

        assertTrue(subscriber1.awaitAllEvents());
        assertTrue(subscriber2.awaitAllEvents());
    }

    @Test
    public void testWorksAsPublisherAndSubscriber() throws InterruptedException {
        reactor.rx.Stream<Buffer> sourceStream = Streams.just(
                Buffer.wrap("Live"),
                Buffer.wrap("Hard"),
                Buffer.wrap("Die"),
                Buffer.wrap("Harder"));
        sourceStream.subscribe(processor);

        SubscriberForTest subscriber = new SubscriberForTest(4);
        processor.subscribe(subscriber);

        assertTrue(subscriber.awaitAllEvents());
        assertTrue(subscriber.awaitCompleted());

        //TODO: Investigate why this is required to avoid JVM failure on Windows
        Thread.sleep(500);
    }

    @Test
    public void testClientReceivesException() throws InterruptedException {
        reactor.rx.Stream<Buffer> sourceStream = Streams.concat(Streams.just(Buffer.wrap("Item")),
                Streams.fail(new RuntimeException("Something went wrong")));
        sourceStream.subscribe(processor);

        SubscriberForTest subscriber = new SubscriberForTest(1);
        processor.subscribe(subscriber);

        assertTrue(subscriber.awaitAllEvents());
        assertTrue(subscriber.awaitError());

        Throwable throwable = subscriber.getLastError();
        assertThat(throwable.getMessage(),
                is("Received an error from the server with message: Something went wrong"));
    }

    @Test
    public void testExceptionWithNullMessageIsHandled() throws InterruptedException {
        reactor.rx.Stream<Buffer> sourceStream = Streams.fail(new RuntimeException());
        sourceStream.subscribe(processor);

        SubscriberForTest subscriber = new SubscriberForTest(0);
        processor.subscribe(subscriber);

        assertTrue(subscriber.awaitError());

        Throwable throwable = subscriber.getLastError();
        assertThat(throwable.getMessage(), is("Received an error from the server with message: "));
    }

    @Test
    public void testOnCompleteShutdownsProcessor() throws InterruptedException {
        reactor.rx.Stream<Buffer> sourceStream = Streams.empty();
        sourceStream.subscribe(processor);

        SubscriberForTest subscriber = new SubscriberForTest(0);
        processor.subscribe(subscriber);

        assertTrue(subscriber.awaitCompleted());

        int timeoutSecs = 5;
        long start = System.nanoTime();
        while(processor.alive()) {
            if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) > timeoutSecs) {
                Assert.fail("Processor didn't shutdown within " + timeoutSecs + " seconds");
            }
            Thread.sleep(100);
        }
    }

    @Test
    public void testTwoProcessorsOnePublisherAnotherSubscriber() throws InterruptedException {
        AeronProcessor p1 = AeronProcessor.create("p1", true, true, CHANNEL, STREAM_ID);
        AeronProcessor p2 = AeronProcessor.create("p2", true, true, CHANNEL, STREAM_ID);
        try {
            SubscriberForTest subscriber2 = new SubscriberForTest(3);
            p2.subscribe(subscriber2);

            p1.onNext(Buffer.wrap("Live"));
            p1.onNext(Buffer.wrap("Hard"));
            p2.onNext(Buffer.wrap("Peace"));

            assertTrue(subscriber2.awaitAllEvents());
        } finally {
            p2.shutdown();
            p1.shutdown();
        }
    }

}