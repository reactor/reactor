/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

import org.reactivestreams.api.Processor;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.tck.IdentityProcessorVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.core.Environment;
import reactor.function.Supplier;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.spec.Promises;
import reactor.rx.spec.Streams;
import reactor.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Stephane Maldini
 */
@Test
public class StreamIdentityProcessorVerification extends IdentityProcessorVerification<Integer> {

	private final Environment env = new Environment();

	public StreamIdentityProcessorVerification() {
		super(new TestEnvironment(2500), 3500);
	}

	@Override
	public Processor<Integer, Integer> createIdentityProcessor(int bufferSize) {
		return
				Action.<Integer>passthrough(env.getDispatcher("ringBuffer"))
				.env(env)
				.prefetch(bufferSize)
				.buffer()
				.map(integer -> integer)
				.distinctUntilChanged()
				.flatMap(Promises::success)
				.scan(Tuple2<Integer, Integer>::getT1)
				.filter(integer -> integer >= 0)
				.collect(1)
				.last()
				.<Integer>split()
				.window(100)
				.<Integer>split()
				.flatMap(integer -> Streams.defer(integer))
				.combine();
	}

	@Override
	public Publisher<Integer> createHelperPublisher(final int elements) {
		if (elements > 0) {
			List<Integer> list = new ArrayList<Integer>(elements);
			for (int i = 1; i <= elements; i++) {
				list.add(i);
			}

			return Streams
					.defer(list, env)
					.filter(integer -> true)
					.map(integer -> integer);

		} else {
			final Random random = new Random();

			return Streams
					.generate((Supplier<Integer>) random::nextInt, env)
					.map(Math::abs);
		}
	}

	@Override
	public Publisher<Integer> createCompletedStatePublisher() {
		Stream<Integer> stream = Streams.defer(env);
		stream.broadcastComplete();
		return stream;
	}

	@Override
	public Publisher<Integer> createErrorStatePublisher() {
		Stream<Integer> stream = Streams.defer(env);
		stream.broadcastError(new Exception("oops"));
		return stream;
	}
}
