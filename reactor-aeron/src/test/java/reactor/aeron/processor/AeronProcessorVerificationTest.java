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
package reactor.aeron.processor;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.IdentityProcessorVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.io.buffer.Buffer;
import reactor.io.net.tcp.support.SocketUtils;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Anatoly Kadyshev
 */
public class AeronProcessorVerificationTest extends IdentityProcessorVerification<Buffer> {

	private String CHANNEL = "udp://localhost:" + SocketUtils.findAvailableUdpPort();

	private int STREAM_ID = 10;

	private AeronProcessor processor;

	/**
	 * Used for streamId generation for every new processor.
	 * Each {@link AeronProcessor} created in the test
	 * should be communicating on different streamIds to avoid interference with previously created instances.
	 */
	int counter = 0;

	public AeronProcessorVerificationTest() {
		super(new TestEnvironment(1100, true), 1100);
	}

	@BeforeClass
	public void doSetup() {
		AeronTestUtils.setAeronEnvProps();
	}

	@AfterMethod
	public void cleanUp(Method method) throws InterruptedException {
		// A previous test didn't call onComplete on the processor, manual clean up
		EmbeddedMediaDriverManager driverManager = EmbeddedMediaDriverManager.getInstance();
		if (driverManager.getCounter() > 0) {

			Thread.sleep(1000);

			if (driverManager.getCounter() > 0) {
				System.err.println("Possibly method " + method.getName() + " didn't call onComplete on processor");

				processor.onComplete();

				Thread.sleep(2000);

				if (driverManager.getCounter() > 0) {
					EmbeddedMediaDriverManager.getInstance().forceShutdown();

					throw new IllegalStateException("Manual termination of processor after method "
							+ method.getName() + " didn't shutdown Media driver");
				}
			}
		}
		processor = null;
	}

	@Override
	public Processor<Buffer, Buffer> createIdentityProcessor(int bufferSize) {
		counter += 4;
		int streamId = STREAM_ID + counter;

		processor = AeronProcessor.create(new Context()
				.name("processor")
				.autoCancel(true)
				.launchEmbeddedMediaDriver(true)
				.channel(CHANNEL)
				.streamId(streamId)
				.errorStreamId(streamId + 1)
				.commandRequestStreamId(streamId + 2)
				.commandReplyStreamId(streamId + 3)
				.publicationLingerTimeoutMillis(250)
				.publicationTimeoutMillis(500)
				.ringBufferSize(1024 * 10));

		return processor;
	}

	@Override
	public Publisher<Buffer> createFailedPublisher() {
		return s -> {
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
				}

				@Override
				public void cancel() {
				}
			});
			s.onError(new Exception("test"));
		};
	}

	@Override
	public ExecutorService publisherExecutorService() {
		return Executors.newCachedThreadPool();
	}

	@Override
	public Buffer createElement(int element) {
		return Buffer.wrap("" + element);
	}

	// Disabled due to Exception comparison by equals
	@Test(enabled = false)
	@Override
	public void mustImmediatelyPassOnOnErrorEventsReceivedFromItsUpstreamToItsDownstream() throws Exception {
		super.mustImmediatelyPassOnOnErrorEventsReceivedFromItsUpstreamToItsDownstream();
	}

	// Disabled due to Exception comparison by equals
	@Test(enabled = false)
	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError()
			throws Throwable {
		super.required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError();
	}

	// Disabled due to Exception comparison by equals
	@Test(enabled = false)
	@Override
	public void required_spec210_mustBePreparedToReceiveAnOnErrorSignalWithoutPrecedingRequestCall()
			throws Throwable {
		super.required_spec210_mustBePreparedToReceiveAnOnErrorSignalWithoutPrecedingRequestCall();
	}

	// Disabled due to Exception comparison by equals
	@Test(enabled = false)
	@Override
	public void required_spec210_mustBePreparedToReceiveAnOnErrorSignalWithPrecedingRequestCall()
			throws Throwable {
		super.required_spec210_mustBePreparedToReceiveAnOnErrorSignalWithPrecedingRequestCall();
	}


	// Disabled due to the fact that it takes some time to subscriber sub2 to initialise.
	// As a result the last line below
	//     expectNextElement(sub2, z);
	// fails as sub2 is hadn't been initialized yet when value z was published.
	//
	// final ManualSubscriber<T> sub2 = newSubscriber();
	// sub1 now has 18 pending
	// sub2 has 0 pending
	// final T z = sendNextTFromUpstream();
	// expectNextElement(sub1, z);
	// sub2.expectNone(); // since sub2 hasn't requested anything yet
	// sub2.request(1);
	// expectNextElement(sub2, z);
	//
	@Override
	@Test(enabled =  false)
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo() throws Throwable {
		super.required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo();
	}

	// Disabled as processor.subscribe request and processor.onComplete are executed in parallel by the test.
	// As a result processor's executor is shutdown by onComplete and as a result .subscribe method fails.
	@Override
	@Test(enabled = false)
	public void required_spec109_mustIssueOnSubscribeForNonNullSubscriber() throws Throwable {
		super.required_spec109_mustIssueOnSubscribeForNonNullSubscriber();
	}

	// TODO: Disabled as Sender part becomes BACKPRESSURED forever as the receiver part is terminated
	@Override
	@Test(enabled = false)
	public void required_spec317_mustNotSignalOnErrorWhenPendingAboveLongMaxValue() throws Throwable {
		super.required_spec317_mustNotSignalOnErrorWhenPendingAboveLongMaxValue();
	}
}
