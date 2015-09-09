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
package reactor.reactivestreams.tck;

import org.junit.Test;
import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.Processors;
import reactor.fn.tuple.Tuple1;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.stream.GroupedStream;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class StreamAndProcessorTests extends AbstractStreamVerification {

	@Override
	public Processor<Integer, Integer> createProcessor(int bufferSize) {

		Stream<String> otherStream = Streams.just("test", "test2", "test3");
		System.out.println("Providing new processor");
		Processor<Integer, Integer> p = Processors.work("stream-raw-fork", bufferSize);

		return
		  Processors.create(
		    p,
		    Streams.wrap(p)
			  .forkJoin(2, (GroupedStream<Integer, Integer> stream) -> stream
				  .scan((prev, next) -> next)
				  .map(integer -> -integer)
				  .filter(integer -> integer <= 0)
				  .sample(1)
				  .map(integer -> -integer)
				  .buffer(batch, 50, TimeUnit.MILLISECONDS)
				  .<Integer>split()
				  .flatMap(i -> Streams.zip(Streams.just(i), otherStream, Tuple1::getT1))
			  )
		      .observe(this::monitorThreadUse)
		      //.log()
		      .process(Processors.async("stream-raw-join", bufferSize))
			  .when(Throwable.class, Throwable::printStackTrace)
		  );
	}

	@Override
	public boolean skipStochasticTests() {
		return true;
	}

	@Override
	public void required_spec309_requestZeroMustSignalIllegalArgumentException() throws Throwable {
		throw new SkipException("TODO");
	}

	@Override
	public void required_spec309_requestNegativeNumberMustSignalIllegalArgumentException() throws Throwable {
		throw new SkipException("TODO");
	}

	@Override
	public void stochastic_spec103_mustSignalOnMethodsSequentially() throws Throwable {
			super.stochastic_spec103_mustSignalOnMethodsSequentially();
	}

	@Test
	public void testHotIdentityProcessor() throws InterruptedException {
			super.testHotIdentityProcessor();
	}


	@Test
	public void testColdIdentityProcessor() throws InterruptedException {
//		for(int i = 0; i < 1000; i++) {
			super.testColdIdentityProcessor();
//		}
	}
}
