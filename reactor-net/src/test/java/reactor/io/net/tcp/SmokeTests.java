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
import org.reactivestreams.Publisher;
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
import reactor.io.net.Spec;
import reactor.io.net.http.HttpChannel;
import reactor.rx.Promise;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author Stephane Maldini
 */
@Ignore
public class SmokeTests {
	private Processor<Buffer, Buffer> processor;
	private reactor.io.net.http.HttpServer<Buffer, Buffer> httpServer;
	private AtomicInteger integer = new AtomicInteger();
	@Test
	public void testMultipleConsumersMultipleTimes() throws Exception {
		Sender sender = new Sender();

		int count = 10_000;
		int threads = 3;
		int fulltotaltext = 0;
		int fulltotalints = 0;
		int iter = 3;

		for (int t=0; t<iter; t++) {
			List<List<String>> clientDatas = getClientDatas(threads, sender, count);

			assertThat(clientDatas.size(), is(threads));

			List<String> numbersNoEnds = new ArrayList<String>();
			List<Integer> numbersNoEndsInt = new ArrayList<Integer>();
			for (int i = 0; i<clientDatas.size(); i++) {
				List<String> datas = clientDatas.get(i);
				assertThat(datas, notNullValue());
				StringBuffer buf = new StringBuffer();
				for (int j = 0; j < datas.size(); j++) {
					buf.append(datas.get(j));
				}

				List<String> split = split(buf.toString());
				for (int x = 0; x < split.size(); x++) {
					String d = split.get(x);
					if (StringUtils.hasText(d) && !d.contains("END")) {
						fulltotaltext += 1;
						numbersNoEnds.add(d);
						int intnum = Integer.parseInt(d);
						if (!numbersNoEndsInt.contains(intnum)) {
							numbersNoEndsInt.add(intnum);
							fulltotalints += 1;
						}
					}
				}
			}

			String msg = "Run number " + t;
			Collections.sort(numbersNoEndsInt);
			System.out.println(numbersNoEndsInt.size()+"/"+(integer.get()*100));
			// we can't measure individual session anymore so just
			// check that below lists match.
			assertThat(msg, numbersNoEndsInt.size(), is(numbersNoEnds.size()));
		}
		// check full totals because we know what this should be
		assertThat(fulltotalints, is(count*iter));
		assertThat(fulltotaltext, is(count*iter));
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
		processor = RingBufferProcessor.create(false);
		Stream<Buffer> bufferStream = Streams
				.wrap(processor)
				//.log("test")
				.window(100, 1, TimeUnit.SECONDS)
				.flatMap(s -> s.reduce(new Buffer(), Buffer::append).observe(d ->
								integer.getAndIncrement()
				))
				.process(RingBufferWorkProcessor.create(false))
				;

//		Stream<Buffer> bufferStream = Streams
//				.wrap(processor)
//				.window(100, 1, TimeUnit.SECONDS)
//				.flatMap(s -> s.dispatchOn(Environment.sharedDispatcher()).reduce(new Buffer(), (prev, next) -> {
//					return prev.append(next);
//				}))
//				.process(RingBufferWorkProcessor.create(false));

		httpServer = NetStreams.httpServer(server -> server
				.codec(new DummyCodec()).listen(8080).dispatcher(Environment.sharedDispatcher()));


		httpServer.get("/data", (request) -> {
			request.responseHeaders().removeTransferEncodingChunked();
			request.addResponseHeader("Content-type", "text/plain");
			request.addResponseHeader("Expires", "0");
			request.addResponseHeader("X-GPFDIST-VERSION", "Spring XD");
			request.addResponseHeader("X-GP-PROTO", "1");
			request.addResponseHeader("Cache-Control", "no-cache");
			request.addResponseHeader("Connection", "close");
			return bufferStream

					.take(5L, TimeUnit.SECONDS)
					.observeComplete(v -> System.out.println("COMPLETE"))
					.concatWith(Streams.just(new Buffer().append("END\n".getBytes(Charset.forName("UTF-8")))))
		});

		httpServer.start().awaitSuccess();
	}

	private Promise<List<String>> getClientDataPromise() throws Exception {
		reactor.io.net.http.HttpClient<String, String> httpClient = NetStreams.httpClient(new Function<Spec.HttpClient<String,String>, Spec.HttpClient<String,String>>() {

			@Override
			public Spec.HttpClient<String, String> apply(Spec.HttpClient<String, String> t) {
				return t.codec(new StringCodec()).connect("localhost", 8080)
						.dispatcher(Environment.sharedDispatcher());
			}
		});
		Promise<List<String>> content = httpClient.get("/data", new Function<HttpChannel<String, String>, Publisher<? extends String>>() {

			@Override
			public Publisher<? extends String> apply(HttpChannel<String, String> t) {
				t.header("Content-Type", "text/plain");
				return Streams.just(" ");
			}
		}).flatMap(new Function<HttpChannel<String, String>, Publisher<? extends List<String>>>() {

			@Override
			public Publisher<? extends List<String>> apply(HttpChannel<String, String> t) {
				return t.toList();
			}
		});

		httpClient.open().awaitSuccess();
		return content;
	}

	private List<List<String>> getClientDatas(int threadCount, final Sender sender, int count) throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final CountDownLatch promiseLatch = new CountDownLatch(threadCount);
		final ArrayList<Thread> joins = new ArrayList<Thread>();
		final ArrayList<List<String>> datas = new ArrayList<List<String>>();


		Runnable srunner = new Runnable() {
			public void run() {
				try {
					sender.sendNext(count);
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			}
		};
		Thread st = new Thread(srunner, "SenderThread" );
		joins.add(st);
		st.start();

		for (int i = 0; i < threadCount; ++i) {
			Runnable runner = new Runnable() {
				public void run() {
					try {
						latch.await();
						Promise<List<String>> clientDataPromise = getClientDataPromise();
						datas.add(clientDataPromise.await(40, TimeUnit.SECONDS));
						promiseLatch.countDown();
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
//				System.out.println("XXXX " + x);
				String data = x++ + "\n";
				processor.onNext(new Buffer().append(data.getBytes(Charset.forName("UTF-8"))).flip());
			}
		}
	}

	public class DummyCodec extends Codec<Buffer, Buffer, Buffer> {

		@SuppressWarnings("resource")
		@Override
		public Buffer apply(Buffer t) {
			System.out.println("XXXXXX" + Thread.currentThread());
			return t.flip();
		}

		@Override
		public Function<Buffer, Buffer> decoder(Consumer<Buffer> next) {
			return null;
		}

	}
}
