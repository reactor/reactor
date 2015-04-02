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
package reactor.io.net.tcp;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Processor;
import reactor.Environment;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.core.support.StringUtils;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.StringCodec;
import reactor.io.net.NetStreams;
import reactor.rx.Promise;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static reactor.Environment.sharedDispatcher;

/**
 * @author Stephane Maldini
 */
@Ignore
public class SmokeTests {
	private Processor<Buffer, Buffer> processor;
	private reactor.io.net.http.HttpServer<Buffer, Buffer> httpServer;

//	Sent 100, expected 100, got 99
//
//	I always seem to miss first number 0. Pay attention to number 32
//	which for some reason is sent first, and is magically on number 0
//	position.
//
//	tried .dispatchOn(Environment.sharedDispatcher()) but it got pretty bad
//
//    +-------------------------------------------------+
//    |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |
//+--------+-------------------------------------------------+----------------+
//|00000000| 33 32 0a 31 0a 32 0a 33 0a 34 0a 35 0a 36 0a 37 |32.1.2.3.4.5.6.7|
//|00000010| 0a 38 0a 39 0a 31 30 0a 31 31 0a 31 32 0a 31 33 |.8.9.10.11.12.13|
//|00000020| 0a 31 34 0a 31 35 0a 31 36 0a 31 37 0a 31 38 0a |.14.15.16.17.18.|
//|00000030| 31 39 0a 32 30 0a 32 31 0a 32 32 0a 32 33 0a 32 |19.20.21.22.23.2|
//|00000040| 34 0a 32 35 0a 32 36 0a 32 37 0a 32 38 0a 32 39 |4.25.26.27.28.29|
//|00000050| 0a 33 30 0a 33 31 0a 33 33 0a 33 34 0a 33 35 0a |.30.31.33.34.35.|
//|00000060| 33 36 0a 33 37 0a 33 38 0a 33 39 0a 34 30 0a 34 |36.37.38.39.40.4|
//|00000070| 31 0a 34 32 0a 34 33 0a 34 34 0a 34 35 0a 34 36 |1.42.43.44.45.46|
//|00000080| 0a 34 37 0a 34 38 0a 34 39 0a 35 30 0a 35 31 0a |.47.48.49.50.51.|
//|00000090| 35 32 0a 35 33 0a 35 34 0a 35 35 0a 35 36 0a 35 |52.53.54.55.56.5|
//|000000a0| 37 0a 35 38 0a 35 39 0a 36 30 0a 36 31 0a 36 32 |7.58.59.60.61.62|
//|000000b0| 0a 36 33 0a 36 34 0a 36 35 0a 36 36 0a 36 37 0a |.63.64.65.66.67.|
//|000000c0| 36 38 0a 36 39 0a 37 30 0a 37 31 0a 37 32 0a 37 |68.69.70.71.72.7|
//|000000d0| 33 0a 37 34 0a 37 35 0a 37 36 0a 37 37 0a 37 38 |3.74.75.76.77.78|
//|000000e0| 0a 37 39 0a 38 30 0a 38 31 0a 38 32 0a 38 33 0a |.79.80.81.82.83.|
//|000000f0| 38 34 0a 38 35 0a 38 36 0a 38 37 0a 38 38 0a 38 |84.85.86.87.88.8|
//|00000100| 39 0a 39 30 0a 39 31 0a 39 32 0a 39 33 0a 39 34 |9.90.91.92.93.94|
//|00000110| 0a 39 35 0a 39 36 0a 39 37 0a 39 38 0a 39 39 0a |.95.96.97.98.99.|
//+--------+-------------------------------------------------+----------------+


	@Test
	public void testMultipleConsumersMultipleTimes() throws Exception {
		Sender sender = new Sender();

		int count = 100;
		int threads = 1;

		for (int t = 0; t < 1; t++) {
			List<List<String>> clientDatas = getClientDatas(threads, sender, count);

			assertThat(clientDatas.size(), is(threads));

			int total = 0;
			List<String> numbersNoEnds = new ArrayList<String>();
			List<Integer> numbersNoEndsInt = new ArrayList<Integer>();
			for (int i = 0; i < clientDatas.size(); i++) {
				List<String> datas = clientDatas.get(i);
				assertThat(datas, notNullValue());
				for (int j = 0; j < datas.size(); j++) {
					String data = datas.get(j);
					List<String> split = split(data);
					for (int x = 0; x < split.size(); x++) {
						if (!split.get(x).contains("END") && !numbersNoEnds.contains(split.get(x))) {
							numbersNoEnds.add(split.get(x));
							try {
								numbersNoEndsInt.add(Integer.parseInt(split.get(x)));
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					total += split.size();
				}
			}

			String msg = "Run number " + t + ", total " + total;
			if (numbersNoEndsInt.size() != count) {
				Collections.sort(numbersNoEndsInt);
				System.out.println(StringUtils.collectionToCommaDelimitedString(numbersNoEndsInt));
			}
			assertThat(msg, numbersNoEnds.size(), is(count));
			// should have total + END with each thread/client
			assertThat(msg, total, is(count + threads));
		}

	}

	@Before
	public void loadEnv() throws Exception {
		Environment.initializeIfEmpty().assignErrorJournal();
		setupFakeProtocolListener();
	}

	@After
	public void clean() throws Exception {
		httpServer.shutdown().awaitSuccess();
	}

	public Set<Integer> findDuplicates(List<Integer> listContainingDuplicates) {
		final Set<Integer> setToReturn = new HashSet<Integer>();
		final Set<Integer> set1 = new HashSet<Integer>();

		for (Integer yourInt : listContainingDuplicates) {
			if (!set1.add(yourInt)) {
				setToReturn.add(yourInt);
			}
		}
		return setToReturn;
	}

	private void setupFakeProtocolListener() throws Exception {
		processor = RingBufferProcessor.create("test", 32);
		Stream<Buffer> bufferStream = Streams
				.wrap(processor)
				.observe(d -> System.out.print("YYY " + d.asString()))
				.window(100, 1, TimeUnit.SECONDS)
				.flatMap(s -> s.reduce(new Buffer(), Buffer::append))
				.process(RingBufferWorkProcessor.create(false));

		httpServer = NetStreams.httpServer(server -> server
				.codec(new DummyCodec()).listen(8080).dispatcher(sharedDispatcher()));

		httpServer.get("/data", (request) -> {
			request.responseHeaders().removeTransferEncodingChunked();
			request.addResponseHeader("Content-type", "text/plain");
			request.addResponseHeader("Expires", "0");
			request.addResponseHeader("X-GPFDIST-VERSION", "Spring XD");
			request.addResponseHeader("X-GP-PROTO", "1");
			request.addResponseHeader("Cache-Control", "no-cache");
			request.addResponseHeader("Connection", "close");
			return bufferStream
					.take(5, TimeUnit.SECONDS)
					.concatWith(Streams.just(new Buffer().append("END\n".getBytes(Charset.forName("UTF-8")))));
		});

		httpServer.start().awaitSuccess();
	}

	private Promise<List<String>> getClientDataPromise() throws Exception {
		reactor.io.net.http.HttpClient<String, String> httpClient = NetStreams.httpClient(t ->
				t.codec(new StringCodec()).connect("localhost", 8080)
						.dispatcher(sharedDispatcher())
		);

		Promise<List<String>> content = httpClient.get("/data", t -> {
				t.header("Content-Type", "text/plain");
				return Streams.just(" ");
			}
		).flatMap(Stream::toList);

		httpClient.open().awaitSuccess();
		return content;
	}

	private List<List<String>> getClientDatas(int threadCount, final Sender sender, int count) throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final CountDownLatch promiseLatch = new CountDownLatch(threadCount);
		final ArrayList<Thread> joins = new ArrayList<Thread>();
		final ArrayList<List<String>> datas = new ArrayList<List<String>>();


		Runnable srunner = () -> {
			try {
				sender.sendNext(count);
			} catch (Exception ie) {
				ie.printStackTrace();
			}
		};

		Thread st = new Thread(srunner, "SenderThread");
		joins.add(st);
		st.start();

		for (int i = 0; i < threadCount; ++i) {
			Runnable runner = () -> {
				try {
					latch.await();
					Promise<List<String>> clientDataPromise = getClientDataPromise();
					promiseLatch.countDown();
					datas.add(clientDataPromise.await(20, TimeUnit.SECONDS));
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			};
			Thread t = new Thread(runner, "SmokeThread" + i);
			joins.add(t);
			t.start();
		}
		latch.countDown();
		promiseLatch.await();
		Thread.sleep(1000);
		for (Thread t : joins) {
			try {
				t.join();
			} catch (InterruptedException e) {
			}
		}

		return datas;
	}

	private static List<String> split(String data) {
		return Arrays.asList(data.split("\\r?\\n"));
	}

	class Sender {
		int x = 0;

		void sendNext(int count) {
			for (int i = 0; i < count; i++) {
				System.out.println("XXXX " + x);
				String data = x++ + "\n";
				processor.onNext(Buffer.wrap(data));
			}
		}
	}

	public class DummyCodec extends Codec<Buffer, Buffer, Buffer> {

		@SuppressWarnings("resource")
		@Override
		public Buffer apply(Buffer t) {
			return t.flip();
		}

		@Override
		public Function<Buffer, Buffer> decoder(Consumer<Buffer> next) {
			return null;
		}

	}
}
