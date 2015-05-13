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
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.core.support.StringUtils;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetStreams;
import reactor.io.net.http.HttpClient;
import reactor.rx.Promise;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author Stephane Maldini
 */
@Ignore
public class ClientServerHttpTests {
	private reactor.io.net.http.HttpServer<List<String>, List<String>> httpServer;
	private Broadcaster<String> broadcaster;

	@Test
	public void testSingleConsumerWithOneSession() throws Exception {
		Sender sender = new Sender();
		sender.sendNext(10);
		List<String> data = getClientData();
		System.out.println(data);
		assertThat(data.size(), is(3));
		assertThat(split(data.get(0)), contains("0", "1", "2", "3", "4"));
		assertThat(split(data.get(1)), contains("5", "6", "7", "8", "9"));
		assertThat(data.get(2), containsString("END"));
		assertThat(data.get(2), is("END\n"));
	}

	@Test
	public void testSingleConsumerWithTwoSession() throws Exception {
		Sender sender = new Sender();
		sender.sendNext(10);
		List<String> data1 = getClientData();

		assertThat(data1.size(), is(3));
		assertThat(split(data1.get(0)), contains("0", "1", "2", "3", "4"));
		assertThat(split(data1.get(1)), contains("5", "6", "7", "8", "9"));
		assertThat(data1.get(2), containsString("END"));
		assertThat(data1.get(2), is("END\n"));

		sender.sendNext(10);
		List<String> data2 = getClientData();

		// we miss batches in between client sessions so this fails
		System.out.println(data2);
		assertThat(data2.size(), is(3));
		assertThat(split(data2.get(0)), contains("10", "11", "12", "13", "14"));
		assertThat(split(data2.get(1)), contains("15", "16", "17", "18", "19"));
		assertThat(data2.get(2), containsString("END"));
		assertThat(data2.get(2), is("END\n"));
	}

	@Test
	public void testSingleConsumerWithTwoSessionBroadcastAfterConnect() throws Exception {
		Sender sender = new Sender();

		final List<String> data1 = new ArrayList<String>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		Runnable runner1 = new Runnable() {
			public void run() {
				try {
					Promise<List<String>> clientDataPromise1 = getClientDataPromise();
					latch1.countDown();
					data1.addAll(clientDataPromise1.await());
				} catch (Exception ie) {
				}
			}
		};

		Thread t1 = new Thread(runner1, "SmokeThread1");
		t1.start();
		latch1.await();
		Thread.sleep(1000);
		sender.sendNext(10);
		t1.join();
		assertThat(data1.size(), is(3));
		assertThat(split(data1.get(0)), contains("0", "1", "2", "3", "4"));
		assertThat(split(data1.get(1)), contains("5", "6", "7", "8", "9"));
		assertThat(data1.get(2), containsString("END"));
		assertThat(data1.get(2), is("END\n"));

		final List<String> data2 = new ArrayList<String>();
		final CountDownLatch latch2 = new CountDownLatch(1);
		Runnable runner2 = new Runnable() {
			public void run() {
				try {
					Promise<List<String>> clientDataPromise2 = getClientDataPromise();
					latch2.countDown();
					data2.addAll(clientDataPromise2.await());
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			}
		};

		Thread t2 = new Thread(runner2, "SmokeThread2");
		t2.start();
		latch2.await();
		Thread.sleep(1000);
		sender.sendNext(10);
		t2.join();
		assertThat(data2.size(), is(3));
		assertThat(split(data2.get(0)), contains("10", "11", "12", "13", "14"));
		assertThat(split(data2.get(1)), contains("15", "16", "17", "18", "19"));
		assertThat(data2.get(2), containsString("END"));
		assertThat(data2.get(2), is("END\n"));
	}

	@Test
	public void testSingleConsumerWithThreeSession() throws Exception {
		Sender sender = new Sender();
		sender.sendNext(10);
		List<String> data1 = getClientData();

		assertThat(data1.size(), is(3));
		assertThat(split(data1.get(0)), contains("0", "1", "2", "3", "4"));
		assertThat(split(data1.get(1)), contains("5", "6", "7", "8", "9"));
		assertThat(data1.get(2), containsString("END"));
		assertThat(data1.get(2), is("END\n"));

		sender.sendNext(10);
		List<String> data2 = getClientData();

		assertThat(data2.size(), is(3));
		assertThat(split(data2.get(0)), contains("10", "11", "12", "13", "14"));
		assertThat(split(data2.get(1)), contains("15", "16", "17", "18", "19"));
		assertThat(data2.get(2), containsString("END"));
		assertThat(data2.get(2), is("END\n"));

		sender.sendNext(10);
		List<String> data3 = getClientData();

		assertThat(data3.size(), is(3));
		assertThat(split(data3.get(0)), contains("20", "21", "22", "23", "24"));
		assertThat(split(data3.get(1)), contains("25", "26", "27", "28", "29"));
		assertThat(data3.get(2), containsString("END"));
		assertThat(data3.get(2), is("END\n"));
	}

	@Test
	public void testMultipleConsumersMultipleTimes() throws Exception {
		Sender sender = new Sender();

		int count = 1000;
		int threads = 5;

		for (int t=0; t<10; t++) {
			List<List<String>> clientDatas = getClientDatas(threads, sender, count);

			assertThat(clientDatas.size(), is(threads));

			int total = 0;
			List<String> numbersNoEnds = new ArrayList<String>();
			List<Integer> numbersNoEndsInt = new ArrayList<Integer>();
			for (int i = 0; i<clientDatas.size(); i++) {
				List<String> datas = clientDatas.get(i);
				assertThat(datas, notNullValue());
				for (int j = 0; j < datas.size(); j++) {
					String data = datas.get(j);
					List<String> split = split(data);
					for (int x = 0; x<split.size(); x++) {
						if (!split.get(x).contains("END") && !numbersNoEnds.contains(split.get(x))) {
							numbersNoEnds.add(split.get(x));
							numbersNoEndsInt.add(Integer.parseInt(split.get(x)));
						}
					}
					total += split.size();
				}
			}

			String msg = "Run number " + t + ", total " + total + " dups=" + StringUtils.collectionToCommaDelimitedString(findDuplicates(numbersNoEndsInt));
			if (numbersNoEndsInt.size() != count) {
				Collections.sort(numbersNoEndsInt);
				System.out.println(StringUtils.collectionToCommaDelimitedString(numbersNoEndsInt));
			}
			assertThat(msg, numbersNoEndsInt.size(), is(count));
			assertThat(msg, numbersNoEnds.size(), is(count));
			// should have total + END with each thread/client
			assertThat(msg, total, is(count+threads));
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
		broadcaster = Broadcaster.<String> create(Environment.sharedDispatcher());
		final Processor<List<String>, List<String>> processor = RingBufferWorkProcessor.create(false);
		broadcaster.buffer(5).subscribe(processor);

		DummyListCodec codec = new DummyListCodec();
		httpServer = NetStreams.httpServer(server -> server
				.codec(codec).listen(0).dispatcher(Environment.sharedDispatcher()));

		httpServer.get("/data", (request) -> {
			request.responseHeaders().removeTransferEncodingChunked();
			return request.writeWith(
					Streams.wrap(processor)
							.log("server")
							.timeout(2, TimeUnit.SECONDS, Streams.empty())
							.concatWith(Streams.just(new ArrayList<String>()))
							.capacity(1L)

			);
		});

		httpServer.start().awaitSuccess();
	}

	private List<String> getClientData() throws Exception {
		return getClientDataPromise().await();
	}

	private Promise<List<String>> getClientDataPromise() throws Exception {
		HttpClient<String, String> httpClient = NetStreams.httpClient(t ->
						t.codec(StandardCodecs.STRING_CODEC).connect("localhost", httpServer.getListenAddress().getPort())
		);

		return httpClient.get("/data" )
				.flatMap(s ->
						s.log("client").toList()
				);
	}

	private List<List<String>> getClientDatas(int threadCount, Sender sender, int count) throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final CountDownLatch promiseLatch = new CountDownLatch(threadCount);
		final ArrayList<Thread> joins = new ArrayList<Thread>();
		final ArrayList<List<String>> datas = new ArrayList<List<String>>();

		for (int i = 0; i < threadCount; ++i) {
			Runnable runner = new Runnable() {
				public void run() {
					try {
						latch.await();
						Promise<List<String>> clientDataPromise = getClientDataPromise();
						promiseLatch.countDown();
						datas.add(clientDataPromise.await(10, TimeUnit.SECONDS));
					} catch (Exception ie) {
						ie.printStackTrace();
					}
				}
			};
			Thread t = new Thread(runner, "SmokeThread" + i);
			joins.add(t);
			t.start();
		}
		latch.countDown();
		promiseLatch.await();
		Thread.sleep(1000);
		sender.sendNext(count);
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
				broadcaster.onNext(x++ + "\n");
			}
		}
	}

	public class DummyListCodec extends Codec<Buffer, List<String>, List<String>> {

		@Override
		public Buffer apply(List<String> t) {
			StringBuffer buf = new StringBuffer();
			if (t.isEmpty()) {
				buf.append("END\n");
			} else {
				for (String n : t) {
					buf.append(n);
				}
			}
			String data = buf.toString();
			return new Buffer().append(data.getBytes()).flip();
		}

		@Override
		public Function<Buffer, List<String>> decoder(Consumer<List<String>> next) {
			return null;
		}
	}
}
