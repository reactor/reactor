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
import org.junit.Before;
import org.junit.Test;
import reactor.io.buffer.Buffer;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Stream;
import reactor.rx.Streams;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronProcessorTest {

	private static final int TIMEOUT_SECS = 1;

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

		Thread.sleep(1000);

		assertThat(EmbeddedMediaDriverManager.getInstance().getCounter(), is(0));

		assertTrue(threadSnapshot.takeAndCompare(new String[] {"hash", "global"},
				TimeUnit.SECONDS.toMillis(TIMEOUT_SECS)));
	}

	private AeronProcessor createProcessor() {
		return AeronProcessor.share(new Context()
				.name("processor")
				.autoCancel(false)
				.launchEmbeddedMediaDriver(true)
				.channel(CHANNEL)
				.streamId(STREAM_ID)
				.errorStreamId(STREAM_ID + 1)
				.commandRequestStreamId(STREAM_ID + 2)
				.commandReplyStreamId(STREAM_ID + 3)
				.publicationLingerTimeoutMillis(50));
	}

	private TestSubscriber createTestSubscriber() {
		return TestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
	}

	@Test
	public void testOnNext() throws InterruptedException {
		processor = createProcessor();

		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);
		subscriber.request(4);

		processor.onNext(Buffer.wrap("Live"));
		processor.onNext(Buffer.wrap("Hard"));
		processor.onNext(Buffer.wrap("Die"));
		processor.onNext(Buffer.wrap("Harder"));
		processor.onNext(Buffer.wrap("Extra"));

		subscriber.assertNextSignals("Live", "Hard", "Die", "Harder");

		subscriber.request(1);
	}

	@Test
	public void testCompleteEventIsPropagated() throws InterruptedException {
		processor = createProcessor();
		Streams.just(
				Buffer.wrap("One"),
				Buffer.wrap("Two"),
				Buffer.wrap("Three"))
				.subscribe(processor);

		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);

		subscriber.request(1);
		subscriber.assertNextSignals("One");

		subscriber.request(1);
		subscriber.assertNextSignals("One", "Two");

		subscriber.request(1);
		subscriber.assertNextSignals("One", "Two", "Three");

		subscriber.assertCompleteReceived();
	}

	@Test
	public void testWorksWithTwoSubscribers() throws InterruptedException {
		processor = createProcessor();
		TestSubscriber subscriber1 = createTestSubscriber();
		processor.subscribe(subscriber1);
		TestSubscriber subscriber2 = createTestSubscriber();
		processor.subscribe(subscriber2);

		subscriber1.requestUnlimited();
		subscriber2.requestUnlimited();

		processor.onNext(Buffer.wrap("Live"));
		processor.onNext(Buffer.wrap("Hard"));
		processor.onNext(Buffer.wrap("Die"));
		processor.onNext(Buffer.wrap("Harder"));

		subscriber1.assertNextSignals("Live", "Hard", "Die", "Harder");
		subscriber2.assertNextSignals("Live", "Hard", "Die", "Harder");
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

		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);
		subscriber.requestUnlimited();

		subscriber.assertNextSignals("Live", "Hard", "Die", "Harder");
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

		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);
		subscriber.requestUnlimited();

		subscriber.assertErrorReceived();

		Throwable throwable = subscriber.getLastError();
		assertThat(throwable.getMessage(), is("Something went wrong"));
	}

	@Test
	public void testExceptionWithNullMessageIsHandled() throws InterruptedException {
		processor = createProcessor();

		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);
		subscriber.requestUnlimited();

		Stream<Buffer> sourceStream = Streams.fail(new RuntimeException());
		sourceStream.subscribe(processor);

		subscriber.assertErrorReceived();

		Throwable throwable = subscriber.getLastError();
		assertThat(throwable.getMessage(), is(""));
	}

	@Test
	public void testOnCompleteShutdownsProcessor() throws InterruptedException {
		processor = createProcessor();
		Stream<Buffer> sourceStream = Streams.empty();
		sourceStream.subscribe(processor);

		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);
		subscriber.requestUnlimited();

		subscriber.assertCompleteReceived();

		TestUtils.waitForTrue(TIMEOUT_SECS, "Processor is still alive", () -> !processor.alive());
	}

	@Test
	public void testOnNextAfterCancel() throws InterruptedException {
		processor = createProcessor();
		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);
		subscriber.requestUnlimited();

		processor.onNext(Buffer.wrap("Hello"));

		subscriber.assertNextSignals("Hello");
		subscriber.cancelSubscription();

		processor.onNext(Buffer.wrap("world"));

		subscriber.assertNextSignals("Hello");
	}

	@Test
	public void testProcessorWorksWithExternalMediaDriver() throws InterruptedException {
		MediaDriver.Context context = new MediaDriver.Context();
		final MediaDriver mediaDriver = MediaDriver.launch(context);
		Aeron.Context ctx = new Aeron.Context();
		ctx.dirName(mediaDriver.contextDirName());
		final Aeron aeron = Aeron.connect(ctx);
		try {
			processor = AeronProcessor.create(new Context()
					.name("processor")
					.autoCancel(false)
					.launchEmbeddedMediaDriver(false)
					.channel(CHANNEL)
					.streamId(STREAM_ID)
					.errorStreamId(STREAM_ID + 1)
					.commandRequestStreamId(STREAM_ID + 2)
					.commandReplyStreamId(STREAM_ID + 3)
					.publicationLingerTimeoutMillis(50)
					.aeron(aeron));

			Streams.just(
					Buffer.wrap("Live"))
					.subscribe(processor);

			TestSubscriber subscriber = createTestSubscriber();
			processor.subscribe(subscriber);
			subscriber.requestUnlimited();

			subscriber.assertNextSignals("Live");
		} finally {
			if (processor != null) {
				processor.awaitAndShutdown(1500, TimeUnit.MILLISECONDS);

				// Waiting to let all Aeron threads to shutdown
				Thread.sleep(500);
			}

			aeron.close();

			mediaDriver.close();

			try {
				System.out.println("Cleaning up media driver files: " + context.dirName());
				context.deleteAeronDirectory();
			} catch (Exception e) {
			}
		}
	}

	@Test
	public void testCreate() throws InterruptedException {
		processor = AeronProcessor.create("aeron", true, CHANNEL);

		Streams.just(
				Buffer.wrap("Live"))
				.subscribe(processor);

		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);
		subscriber.requestUnlimited();

		subscriber.assertNextSignals("Live");
		subscriber.assertCompleteReceived();
	}

	@Test
	public void testShare() throws InterruptedException {
		processor = AeronProcessor.share("aeron", true, CHANNEL);

		Streams.just(
				Buffer.wrap("Live"))
				.subscribe(processor);

		TestSubscriber subscriber = createTestSubscriber();
		processor.subscribe(subscriber);
		subscriber.requestUnlimited();

		subscriber.assertNextSignals("Live");
		subscriber.assertCompleteReceived();
	}
}