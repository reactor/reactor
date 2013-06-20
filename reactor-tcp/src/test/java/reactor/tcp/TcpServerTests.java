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

package reactor.tcp;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.fn.dispatch.SynchronousDispatcher;
import reactor.tcp.config.ServerSocketOptions;
import reactor.tcp.encoding.LengthFieldCodec;
import reactor.tcp.encoding.StandardCodecs;
import reactor.tcp.encoding.json.JsonCodec;
import reactor.tcp.netty.NettyTcpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Jon Brisbin
 */
@Ignore
public class TcpServerTests {

	final ExecutorService threadPool = Executors.newCachedThreadPool();
	final int             msgs       = 4000000;
	final int             threads    = 4;
	final int             port       = 24887;

	Environment    env;
	CountDownLatch latch;
	AtomicLong count = new AtomicLong();
	AtomicLong start = new AtomicLong();
	AtomicLong end   = new AtomicLong();

	@Before
	public void loadEnv() {
		env = new Environment();
		latch = new CountDownLatch(msgs * threads);
	}

	@After
	public void cleanup() {
		threadPool.shutdownNow();
	}

	@Test
	public void tcpServerHandlesJsonPojos() throws InterruptedException {
		TcpServer<Pojo, Pojo> server = new TcpServer.Spec<Pojo, Pojo>(NettyTcpServer.class)
				.using(env)
				.dispatcher(Environment.EVENT_LOOP)
				.codec(new JsonCodec<Pojo, Pojo>(Pojo.class, Pojo.class))
				.consume(new Consumer<TcpConnection<Pojo, Pojo>>() {
					@Override
					public void accept(TcpConnection<Pojo, Pojo> conn) {
						conn.consume(new Consumer<Pojo>() {
							@Override
							public void accept(Pojo data) {
								System.out.println("got data: " + data);
							}
						});
					}
				})
				.get()
				.start();

		while (true) {
			Thread.sleep(5000);
		}
	}

	@Test
	public void tcpServerHandlesLengthFieldData() throws InterruptedException {
		TcpServer<byte[], byte[]> server = new TcpServer.Spec<byte[], byte[]>(NettyTcpServer.class)
				.using(env)
				.using(SynchronousDispatcher.INSTANCE)
				.options(new ServerSocketOptions()
										 .backlog(1000)
										 .reuseAddr(true)
										 .tcpNoDelay(true))
				.listen(port)
				.codec(new LengthFieldCodec<byte[], byte[]>(StandardCodecs.BYTE_ARRAY_CODEC))
				.consume(new Consumer<TcpConnection<byte[], byte[]>>() {
					@Override
					public void accept(TcpConnection<byte[], byte[]> conn) {
						conn.consume(new Consumer<byte[]>() {
							long num = 1;

							@Override
							public void accept(byte[] bytes) {
								latch.countDown();
								ByteBuffer bb = ByteBuffer.wrap(bytes);
								if (bb.remaining() < 4) {
									System.err.println("insufficient len: " + bb.remaining());
								}
								int next = bb.getInt();
								if (next != num++) {
									System.err.println(this + " expecting: " + next + " but got: " + (num - 1));
								}
							}
						});
					}
				})
				.get()
				.start(new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						start.set(System.currentTimeMillis());
						for (int i = 0; i < threads; i++) {
							threadPool.submit(new LengthFieldMessageWriter(port));
						}
					}
				});

		latch.await(30, TimeUnit.SECONDS);
		end.set(System.currentTimeMillis());

		assertThat("latch was counted down", latch.getCount(), is(0L));

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		server.shutdown();
	}

	private static class Pojo {
		String name;

		private Pojo() {
		}

		private Pojo(String name) {
			this.name = name;
		}

		private String getName() {
			return name;
		}

		private void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "Pojo{" +
					"name='" + name + '\'' +
					'}';
		}
	}

	private class LengthFieldMessageWriter extends Thread {
		private final Random rand = new Random();
		private final int port;
		private final int length;

		private LengthFieldMessageWriter(int port) {
			this.port = port;
			this.length = rand.nextInt(256);
		}

		@Override
		public void run() {
			try {
				java.nio.channels.SocketChannel ch = java.nio.channels.SocketChannel.open(new InetSocketAddress(port));

				System.out.println("writing " + msgs + " messages of " + length + " byte length...");

				int num = 1;
				start.set(System.currentTimeMillis());
				for (int j = 0; j < msgs; j++) {
					ByteBuffer buff = ByteBuffer.allocate(length + 4);
					buff.putInt(length);
					buff.putInt(num++);
					buff.position(0);
					buff.limit(length + 4);

					ch.write(buff);

					count.incrementAndGet();
				}
			} catch (IOException e) {
			}
		}
	}

}
