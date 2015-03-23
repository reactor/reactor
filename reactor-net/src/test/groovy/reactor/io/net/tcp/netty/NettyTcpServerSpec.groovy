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

package reactor.io.net.tcp.netty

import reactor.Environment
import reactor.io.buffer.Buffer
import reactor.io.codec.PassThroughCodec
import reactor.io.codec.json.JsonCodec
import reactor.io.net.NetStreams
import reactor.io.net.tcp.support.SocketUtils
import reactor.rx.Streams
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class NettyTcpServerSpec extends Specification {

	static final int port = SocketUtils.findAvailableTcpPort()

	def "NettyTcpServer responds to requests from clients"() {
		given: "a simple TcpServer"
			def env = Environment.initializeIfEmpty()
			def stopLatch = new CountDownLatch(1)
			def dataLatch = new CountDownLatch(1)
			def server = NetStreams.<Buffer, Buffer> tcpServer {
				it.
						env(env).
						listen(port).
						codec(new PassThroughCodec<Buffer>())
			}

		when: "the server is started"
			server.consume { conn ->
				conn.sinkBuffers(Streams.just(Buffer.wrap("Hello World!")))
			}

		then: "the server was started"
			server.start().awaitSuccess()

		when: "data is sent"
			def client = new SimpleClient(port, dataLatch, Buffer.wrap("Hello World!"))
			client.start()
			dataLatch.await(5, TimeUnit.SECONDS)

		then: "data was recieved"
			client.data?.remaining() == 12
			new Buffer(client.data).asString() == "Hello World!"
			dataLatch.count == 0

		cleanup: "the server is stopped"
			server.shutdown().onSuccess {
				stopLatch.countDown()
			}
			stopLatch.await(5, TimeUnit.SECONDS)
			Environment.terminate()
	}

	def "NettyTcpServer can encode and decode JSON"() {
		given: "a TcpServer with JSON defaultCodec"
			def env = Environment.initializeIfEmpty()
			def stopLatch = new CountDownLatch(1)
			def dataLatch = new CountDownLatch(1)
			def server = NetStreams.<Pojo, Pojo> tcpServer {
				it.env(env).
						listen(port).
						codec(new JsonCodec<Pojo, Pojo>(Pojo))
			}

		when: "the server is started"
			server.pipeline { conn ->
				conn.map { pojo ->
					assert pojo.name == "John Doe"
					new Pojo(name: "Jane Doe")
				}
			}

		then: "the server was started"
			server.start().awaitSuccess(50, TimeUnit.SECONDS)

		when: "a pojo is written"
			def client = new SimpleClient(port, dataLatch, Buffer.wrap("{\"name\":\"John Doe\"}"))
			client.start()
			dataLatch.await(5, TimeUnit.SECONDS)

		then: "data was recieved"
			client.data?.remaining() == 19
			new Buffer(client.data).asString() == "{\"name\":\"Jane Doe\"}"
			dataLatch.count == 0

		cleanup: "the server is stopped"
			server.shutdown().onSuccess {
				stopLatch.countDown()
			}
			stopLatch.await(5, TimeUnit.SECONDS)
			Environment.terminate()
	}

	def "flush every 5 elems with manual decoding"() {
		given: "a TcpServer and a TcpClient"
			def env = Environment.initializeIfEmpty()
			def latch = new CountDownLatch(10)

			def server = NetStreams.tcpServer(port)
			def client = NetStreams.tcpClient("localhost", port)
			def codec = new JsonCodec<Pojo, Pojo>(Pojo)

		when: "the client/server are prepared"
			server.pipeline { input ->
				input
						.decode(codec)
						.log('serve')
						.map(codec)
						.capacity(5l)
			}

			client.pipeline { input ->
				input
						.decode(codec)
						.log('receive')
						.consume { latch.countDown() }

				Streams.range(1, 10)
						.map { new Pojo(name: 'test' + it) }
						.log('send')
						.map(codec)
						.capacity(10l)
			}

		then: "the client/server were started"
			server?.start()?.flatMap { client.open() }?.awaitSuccess(5, TimeUnit.SECONDS)
			latch.await(10, TimeUnit.SECONDS)


		cleanup: "the client/server where stopped"
			client?.close()?.flatMap { server.shutdown() }?.awaitSuccess(5, TimeUnit.SECONDS)
			Environment.terminate()
	}


	def "retry strategies when server fails"() {
		given: "a TcpServer and a TcpClient"
			def env = Environment.initializeIfEmpty()
			def latch = new CountDownLatch(10)

			def server = NetStreams.tcpServer(port)
			def client = NetStreams.tcpClient("localhost", port)
			def codec = new JsonCodec<Pojo, Pojo>(Pojo)
			def i = 0

		when: "the client/server are prepared"
			server.pipeline { input ->
				input
						.decode(codec)
						.flatMap {
							Streams.just(it)
								.log('serve')
								.observe {
									if (i++ < 2) {
										throw new Exception("test")
									}
								}
								.retry(2)
						}
						.map(codec)
			}

			client.pipeline { input ->
				input
						.decode(codec)
						.log('receive')
						.consume { latch.countDown() }

				Streams.range(1, 10)
						.map { new Pojo(name: 'test' + it) }
						.log('send')
						.map(codec)
			}

		then: "the client/server were started"
			server?.start()?.flatMap { client.open() }?.awaitSuccess(5, TimeUnit.SECONDS)
			latch.await(10, TimeUnit.SECONDS)


		cleanup: "the client/server where stopped"
			client?.close()?.flatMap { server.shutdown() }?.awaitSuccess(5, TimeUnit.SECONDS)
			Environment.terminate()
	}


	static class SimpleClient extends Thread {
		final int port
		final CountDownLatch latch
		final Buffer output
		ByteBuffer data

		SimpleClient(int port, CountDownLatch latch, Buffer output) {
			this.port = port
			this.latch = latch
			this.output = output
		}

		@Override
		void run() {
			def ch = SocketChannel.open(new InetSocketAddress(port))
			def len = ch.write(output.byteBuffer())
			assert ch.connected
			data = ByteBuffer.allocate(len)
			int read = ch.read(data)
			assert read > 0
			data.flip()
			latch.countDown()
		}
	}

	static class Pojo {
		String name

		@Override
		String toString() {
			name
		}
	}

}
