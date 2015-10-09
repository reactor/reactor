/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.AbstractReactorTest;
import reactor.fn.Consumer;
import reactor.fn.tuple.Tuple2;
import reactor.rx.action.Control;
import reactor.rx.broadcast.Broadcaster;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Stephane Maldini
 */
public class StreamCombinationTests extends AbstractReactorTest {

	private static final Logger LOG = LoggerFactory.getLogger(StreamCombinationTests.class);

	private ArrayList<Stream<SensorData>> allSensors;
	private Broadcaster<SensorData>       sensorEven;
	private Broadcaster<SensorData>       sensorOdd;

	@Before
	public void before() {
		sensorEven();
		sensorOdd();
	}

	@After
	public void after() {
	}

	public Consumer<Object> loggingConsumer() {
		return m -> LOG.info("(int) msg={}", m);
	}

	public List<Stream<SensorData>> allSensors() {
		if (allSensors == null) {
			this.allSensors = new ArrayList<>();
		}
		return allSensors;
	}

	public Stream<SensorData> sensorOdd() {
		if (sensorOdd == null) {
			// this is the stream we publish odd-numbered events to
			this.sensorOdd = Broadcaster.<SensorData>create();

			// add substream to "master" list
			//allSensors().add(sensorOdd.reduce(this::computeMin).timeout(1000));
		}

		return sensorOdd.dispatchOn(asyncGroup).log("odd");
	}

	public Stream<SensorData> sensorEven() {
		if (sensorEven == null) {
			// this is the stream we publish even-numbered events to
			this.sensorEven = Broadcaster.<SensorData>create();

			// add substream to "master" list
			//allSensors().add(sensorEven.reduce(this::computeMin).timeout(1000));
		}

		return sensorEven.dispatchOn(asyncGroup).log("even");
	}

	@Test
	public void testMerge1ToN() throws Exception {
		final int n = 1000;


		Stream<Integer> stream = Streams.merge(
		  Streams.just(1)
			.map(i -> Streams.range(0, n))
		);


		final CountDownLatch latch = new CountDownLatch(n);
		awaitLatch(stream.consume(integer -> latch.countDown()), latch);
	}

	@Test
	public void sampleMergeWithTest() throws Exception {
		int elements = 40;
		CountDownLatch latch = new CountDownLatch(elements);

		Control tail = sensorOdd().log().mergeWith(sensorEven())
		  .observe(loggingConsumer())
		  .consume(i -> latch.countDown());

		generateData(elements);

		awaitLatch(tail, latch);
	}

	/*@Test
	public void sampleConcatTestConsistent() throws Exception {
		for(int i = 0; i < 1000; i++){
			System.out.println("------");
			sampleConcatTest();
		}
	}*/

	@Test
	public void sampleConcatTest() throws Exception {
		int elements = 40;

		CountDownLatch latch = new CountDownLatch(elements + 1);

		Control tail = Streams.concat(sensorOdd(), sensorEven().cache())
		  .log("concat")
		  .consume(i -> latch.countDown(), null, nothing -> latch.countDown());

		System.out.println(tail.debug());
		generateData(elements);

		sensorEven.onComplete();
		sensorOdd.onComplete();

		awaitLatch(tail, latch);
	}


	@Test
	public void sampleCombineLatestTest() throws Exception {
		int elements = 40;
		CountDownLatch latch = new CountDownLatch(elements / 2 + 1);

		Control tail = Streams.combineLatest(sensorOdd(), sensorEven(), this::computeMin)
		  .log("combineLatest")
		  .consume(i -> latch.countDown(), null, nothing -> latch.countDown());

		generateData(elements);

		sensorEven.onComplete();
		sensorOdd.onComplete();

		awaitLatch(tail, latch);
	}

	@Test
	public void concatWithTest() throws Exception {
		int elements = 40;
		CountDownLatch latch = new CountDownLatch(elements + 1);

		Control tail = sensorOdd().concatWith(sensorEven().cache())
		  .log("concat")
		  .consume(i -> latch.countDown(), null, nothing -> latch.countDown());

		generateData(elements);

		sensorEven.onComplete();
		sensorOdd.onComplete();

		awaitLatch(tail, latch);
	}

	@Test
	public void zipWithTest() throws Exception {
		int elements = 40;
		CountDownLatch latch = new CountDownLatch(elements / 2);

		Control tail = sensorOdd().zipWith(sensorEven(), this::computeMin)
		  .log("zipWithTest")
		  .consume(i -> latch.countDown());

		generateData(elements);

		awaitLatch(tail, latch);
	}

	@Test
	public void zipWithIterableTest() throws Exception {
		int elements = 31;
		CountDownLatch latch = new CountDownLatch((elements / 2) - 1);

		List<Integer> list = IntStream.range(0, elements / 2)
		  .boxed()
		  .collect(Collectors.toList());

		LOG.info("range from 0 to " + list.size());
		Control tail = sensorOdd().zipWith(list, (tuple) -> (tuple.getT1().toString() +
		  "" +
		  " " +
		  "-- " + tuple.getT2()))
		  .log("zipWithIterableTest")
		  .consume(i -> latch.countDown());

		System.out.println(tail.debug());
		generateData(elements);

		awaitLatch(tail, latch);
	}

	@Test
	public void joinWithTest() throws Exception {
		int elements = 40;
		CountDownLatch latch = new CountDownLatch(elements / 2);

		Control tail = sensorOdd().joinWith(sensorEven())
		  .log("joinWithTest")
		  .consume(i -> latch.countDown());

		generateData(elements);

		awaitLatch(tail, latch);
	}

	@Test
	public void sampleZipTest() throws Exception {
		int elements = 69;
		CountDownLatch latch = new CountDownLatch(elements / 2);

		Control tail = Streams.zip(sensorEven(), sensorOdd(), this::computeMin)
		  .log("sampleZipTest")
		  .consume(x -> latch.countDown());

		generateData(elements);

		awaitLatch(tail, latch);
	}

	@SuppressWarnings("unchecked")
	private void awaitLatch(Control tail, CountDownLatch latch) throws Exception {
		if (!latch.await(10, TimeUnit.SECONDS)) {
			throw new Exception("Never completed: (" + latch.getCount() + ") "  + tail.debug());
		}
	}

	private void generateData(int elements) {
		Random random = new Random();
		SensorData data;
		Broadcaster<SensorData> upstream;

		for (long i = 0; i < elements; i++) {
			data = new SensorData(i, random.nextFloat() * 100);
			if (i % 2 == 0) {
				upstream = sensorEven;
			} else {
				upstream = sensorOdd;
			}
			if(upstream.downstreamSubscription() != null) {
				upstream.onNext(data);
			}
		}

	}

	private SensorData computeMin(Tuple2<SensorData, SensorData> tuple) {
		SensorData sd1 = tuple.getT1();
		SensorData sd2 = tuple.getT2();
		return (null != sd2 ? (sd2.getValue() < sd1.getValue() ? sd2 : sd1) : sd1);
	}

	public class SensorData implements Comparable<SensorData> {

		private final Long  id;
		private final Float value;

		public SensorData(Long id, Float value) {
			this.id = id;
			this.value = value;
		}

		public Long getId() {
			return id;
		}

		public Float getValue() {
			return value;
		}

		@Override
		public int compareTo(SensorData other) {
			if (null == other) {
				return 1;
			}
			return value.compareTo(other.getValue());
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof SensorData)) {
				return false;
			}
			SensorData other = (SensorData) obj;
			return (Long.compare(other.getId(), id) == 0)
			  && (Float.compare(other.getValue(), value) == 0);
		}

		@Override
		public int hashCode() {
			return id != null ? id.hashCode() : 0;
		}

		@Override
		public String toString() {
			return "SensorData{" +
			  "id=" + id +
			  ", value=" + value +
			  '}';
		}
	}
}
