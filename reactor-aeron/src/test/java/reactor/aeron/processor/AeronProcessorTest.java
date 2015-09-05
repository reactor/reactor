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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.io.buffer.Buffer;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Stream;
import reactor.rx.Streams;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronProcessorTest {

	private static final int TIMEOUT_SECS = 5;

	private String CHANNEL = "udp://localhost:" + SocketUtils.findAvailableUdpPort();

	private int STREAM_ID = 10;

	private AeronProcessor processor;

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTearDown() throws InterruptedException {
		if (processor != null) {
			processor.shutdown();
		}

		assertTrue(threadSnapshot.takeAndCompare(new String[] {"hash"}, TimeUnit.SECONDS.toMillis(TIMEOUT_SECS)));
	}

	private AeronProcessor createProcessor() {
		return AeronProcessor.builder()
				.name("processor")
				.autoCancel(false)
				.launchEmbeddedMediaDriver(true)
				.channel(CHANNEL)
				.streamId(STREAM_ID)
				.errorStreamId(STREAM_ID + 1)
				.commandRequestStreamId(STREAM_ID + 2)
				.commandReplyStreamId(STREAM_ID + 3)
				.publicationLingerTimeoutMillis(50)
				.share();
	}

	private TestSubscriber createTestSubscriber(int nExpectedEvents) {
		return new TestSubscriber(TIMEOUT_SECS, nExpectedEvents);
	}

	@Test
	public void testOnNext() throws InterruptedException {
		processor = createProcessor();
		TestSubscriber subscriber = createTestSubscriber(4);
		processor.subscribe(subscriber);

		processor.onNext(Buffer.wrap("Live"));
		processor.onNext(Buffer.wrap("Hard"));
		processor.onNext(Buffer.wrap("Die"));
		processor.onNext(Buffer.wrap("Harder"));

		subscriber.assertAllEventsReceived();
	}

	@Test
	public void testWorksWithTwoSubscribers() throws InterruptedException {
		processor = createProcessor();
		TestSubscriber subscriber1 = createTestSubscriber(4);
		processor.subscribe(subscriber1);
		TestSubscriber subscriber2 = createTestSubscriber(4);
		processor.subscribe(subscriber2);

		processor.onNext(Buffer.wrap("Live"));
		processor.onNext(Buffer.wrap("Hard"));
		processor.onNext(Buffer.wrap("Die"));
		processor.onNext(Buffer.wrap("Harder"));

		subscriber1.assertAllEventsReceived();
		subscriber2.assertAllEventsReceived();
	}

	@Test
	public void testWorksAsPublisherAndSubscriber() throws InterruptedException {
		processor = createProcessor();
		Streams.just(
				Buffer.wrap("Live"),
				Buffer.wrap("Hard"),
				Buffer.wrap("Die"),
				Buffer.wrap("Harder"))
			.subscribe(processor);

		TestSubscriber subscriber = createTestSubscriber(4);
		processor.subscribe(subscriber);

		subscriber.assertAllEventsReceived();
		subscriber.assertCompleteReceived();
	}

	@Test
	public void testClientReceivesException() throws InterruptedException {
		processor = createProcessor();
		// as error is delivered on a different channelId compared to signal
		// its delivery could shutdown the processor before the processor subscriber
		// receives signal
		Streams.concat(Streams.just(Buffer.wrap("Item")),
				Streams.fail(new RuntimeException("Something went wrong")))
			.subscribe(processor);

		TestSubscriber subscriber = createTestSubscriber(1);
		processor.subscribe(subscriber);

		subscriber.awaitError();

		Throwable throwable = subscriber.getLastError();
		assertThat(throwable.getMessage(), is("Something went wrong"));
	}

	@Test
	public void testExceptionWithNullMessageIsHandled() throws InterruptedException {
		processor = createProcessor();
		Stream<Buffer> sourceStream = Streams.fail(new RuntimeException());
		sourceStream.subscribe(processor);

		TestSubscriber subscriber = createTestSubscriber(0);
		processor.subscribe(subscriber);

		subscriber.awaitError();

		Throwable throwable = subscriber.getLastError();
		assertThat(throwable.getMessage(), is(""));
	}

	@Test
	public void testOnCompleteShutdownsProcessor() throws InterruptedException {
		processor = createProcessor();
		Stream<Buffer> sourceStream = Streams.empty();
		sourceStream.subscribe(processor);

		TestSubscriber subscriber = createTestSubscriber(0);
		processor.subscribe(subscriber);

		subscriber.assertCompleteReceived();

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
	public void testOnNextAfterCancel() throws InterruptedException {
		processor = createProcessor();
		TestSubscriber subscriber = createTestSubscriber(1);
		processor.subscribe(subscriber);

		processor.onNext(Buffer.wrap("Hello"));

		subscriber.assertAllEventsReceived();
		subscriber.cancel();

		processor.onNext(Buffer.wrap("world"));
	}

	@Test
	public void testProcessorWorksWithExternalMediaDriver() throws InterruptedException {
		MediaDriver.Context context = new MediaDriver.Context();
		final MediaDriver mediaDriver = MediaDriver.launch(context);
		try {
			processor = AeronProcessor.builder()
					.name("processor")
					.autoCancel(false)
					.launchEmbeddedMediaDriver(false)
					.channel(CHANNEL)
					.streamId(STREAM_ID)
					.errorStreamId(STREAM_ID + 1)
					.commandRequestStreamId(STREAM_ID + 2)
					.commandReplyStreamId(STREAM_ID + 3)
					.publicationLingerTimeoutMillis(50)
					.create();

			Streams.just(
					Buffer.wrap("Live"))
					.subscribe(processor);

			TestSubscriber subscriber = createTestSubscriber(1);
			processor.subscribe(subscriber);

			subscriber.assertAllEventsReceived();
		} finally {
			if (processor != null) {
				processor.shutdown();
				Thread.sleep(500);
			}

			mediaDriver.close();

			try {
				System.out.println("Cleaning up media driver files: " + context.dirName());
				context.deleteAeronDirectory();
			} catch (Exception e) {
			}
		}
	}
}